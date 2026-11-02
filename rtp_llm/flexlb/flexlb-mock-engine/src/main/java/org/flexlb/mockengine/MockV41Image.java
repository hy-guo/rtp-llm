package org.flexlb.mockengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import org.flexlb.engine.grpc.EngineRpcService;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

/** main DeepSeekV41VisionEmbedding.validate_prepared_images contract; CPU forward only. */
final class MockV41Image {
    record Config(int hidden, int patch, int ratio, int maxTokens, int maxSequence, String identity,
                  int minPixels, int imageTokenId) {
        Config(int hidden, int patch, int ratio, int maxTokens, int maxSequence, String identity) {
            this(hidden, patch, ratio, maxTokens, maxSequence, identity, 295936, 129264);
        }
        static Config fromModel(Path model) throws Exception {
            var mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(model.resolve("config.json").toFile());
            JsonNode text = root.required("text_config"), vision = root.required("vision_config");
            var fields = new TreeMap<String, JsonNode>();
            fields.put("vision_patch_size", vision.required("patch_size"));
            fields.put("vision_downsample_ratio", vision.required("downsample_ratio"));
            fields.put("vision_max_n_token", vision.required("max_image_tokens"));
            fields.put("vision_min_pixels", vision.required("min_pixels"));
            fields.put("vision_max_wh_ratio", vision.path("max_wh_ratio").isMissingNode()
                    ? mapper.nullNode() : vision.get("max_wh_ratio"));
            fields.put("image_token_id", root.required("image_token_id"));
            fields.put("vocab_size", text.required("vocab_size"));
            fields.put("max_seq_len", text.required("max_position_embeddings"));
            // Python json.dumps(asdict(processor), sort_keys=True), including separators.
            var parts = new ArrayList<String>();
            for (var field : fields.entrySet()) parts.add(mapper.writeValueAsString(field.getKey())
                    + ": " + mapper.writeValueAsString(field.getValue()));
            String canonical = "{" + String.join(", ", parts) + "}";
            String identity = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8)));
            return new Config(text.required("hidden_size").asInt(), vision.required("patch_size").asInt(),
                    vision.required("downsample_ratio").asInt(), vision.required("max_image_tokens").asInt(),
                    text.required("max_position_embeddings").asInt(), identity,
                    vision.required("min_pixels").asInt(), root.required("image_token_id").asInt());
        }
        Config {
            if (hidden <= 0 || patch <= 0 || ratio <= 0 || maxTokens <= 0 || maxSequence <= 0
                    || minPixels <= 0 || imageTokenId < 0
                    || identity == null || !identity.matches("[0-9a-f]{64}"))
                throw new IllegalArgumentException("invalid V4.1 model metadata");
        }
    }
    record Work(String key, EngineRpcService.V41ImageInputPB image) {}

    // deepseek_v41_processor.plan_image_grid, including extreme aspect ratios.
    static int[] grid(int width, int height, Config config) {
        if (width <= 0 || height <= 0) throw new IllegalArgumentException("invalid image dimensions");
        int patch = config.patch(), ratio = config.ratio();
        if ((long) width * height < config.minPixels()) {
            double scale = Math.sqrt((double) config.minPixels() / ((long) width * height));
            width = (int) (width * scale);
            height = (int) (height * scale);
        }
        int bestW = Math.toIntExact(((long) width + patch - 1) / patch * patch);
        int bestH = Math.toIntExact(((long) height + patch - 1) / patch * patch);
        long h = ((long) bestH / patch + ratio - 1) / ratio;
        long w = ((long) bestW / patch + ratio - 1) / ratio;
        if (h * (w + 1) + 2 > config.maxTokens()) {
            double aspect = (double) height / width;
            double maxW = Math.sqrt((config.maxTokens() - 2) / aspect + .25) - .5;
            double maxH = maxW * aspect;
            int cell = Math.multiplyExact(patch, ratio);
            if (maxW < 1) {
                bestH = Math.multiplyExact((config.maxTokens() - 2) / 2, cell);
                bestW = cell;
            } else if (maxH < 1) {
                bestH = cell;
                bestW = Math.multiplyExact(config.maxTokens() - 3, cell);
            } else {
                double scale = Math.min(Math.floor(maxW) * cell / width, Math.floor(maxH) * cell / height);
                bestH = (int) Math.floor(height * scale / patch) * patch;
                bestW = (int) Math.floor(width * scale / patch) * patch;
            }
        }
        return new int[] {bestH / patch, bestW / patch};
    }

    static MockVitImage.Result compute(EngineRpcService.MultimodalInputPB input, Config config,
                                      int maxInputBytes, long maxOutputBytes, long timeoutMs) throws Exception {
        var image = MockVitImage.decode(input, maxInputBytes, timeoutMs);
        int[] grid = grid(image.getWidth(), image.getHeight(), config);
        int rows = types(grid[0], grid[1], config.ratio()).size();
        var digest = MessageDigest.getInstance("SHA-256");
        digest.update(config.identity().getBytes(StandardCharsets.UTF_8));
        digest.update(ByteBuffer.allocate(8).putInt(grid[0]).putInt(grid[1]).array());
        for (int y = 0; y < image.getHeight(); y++) for (int x = 0; x < image.getWidth(); x++) {
            int rgb = image.getRGB(x, y);
            digest.update((byte) (rgb >>> 16)); digest.update((byte) (rgb >>> 8)); digest.update((byte) rgb);
        }
        return simulatedForward(rows, config, maxOutputBytes, digest.digest());
    }

    static List<Integer> types(int height, int width, int ratio) {
        long h = ((long)height + ratio - 1) / ratio, w = ((long)width + ratio - 1) / ratio;
        if (height <= 0 || width <= 0 || h * (w + 1) + 2 > 1_048_576)
            throw new IllegalArgumentException("invalid V4.1 image grid");
        var result = new ArrayList<Integer>();
        result.add(0);
        for (int y = 0; y < h; y++) {
            for (int x = 0; x < w; x++) result.add(1);
            result.add(2);
        }
        result.add(3);
        return result;
    }

    static List<Work> validate(EngineRpcService.MultimodalInputsPB inputs, Config config, int maxInputBytes) {
        var typed = inputs.getV41Inputs();
        if (config == null) throw new IllegalArgumentException("V4.1 requires --model-path matching frontend");
        if (typed.getSchemaVersion() != 1 || inputs.getMultimodalInputsCount() != 0
                || typed.getImagesCount() > 256 || inputs.getSerializedSize() > maxInputBytes)
            throw new IllegalArgumentException("invalid V4.1 request schema, mixed inputs or byte limit");
        long previousEnd = 0;
        var work = new ArrayList<Work>();
        for (var image : typed.getImagesList()) {
            var expected = types(image.getNVitH(), image.getNVitW(), config.ratio());
            var patches = image.getPatches();
            long n = (long)image.getNVitH() * image.getNVitW();
            long bytes = Math.multiplyExact(n, (long)3 * config.patch() * config.patch() * 2);
            if (expected.size() > config.maxTokens() || !expected.equals(image.getTypesList())
                    || !config.identity().equals(image.getProcessorIdentity())
                    || image.getStart() < previousEnd
                    || (long)image.getStart() + expected.size() >= config.maxSequence()
                    || patches.getDataType() != EngineRpcService.TensorPB.DataType.BF16
                    || !patches.getShapeList().equals(List.of(n, 3L, (long)config.patch(), (long)config.patch()))
                    || patches.getBf16Data().size() != bytes)
                throw new IllegalArgumentException("invalid V4.1 patches, processor identity or image types");
            previousEnd = (long)image.getStart() + expected.size();
            String content = image.getContentSha256();
            if (!content.isEmpty() && !content.matches("[0-9a-f]{64}"))
                throw new IllegalArgumentException("invalid V4.1 content identity");
            // Real vision encode cache trusts nonempty content identity and bypasses empty hashes.
            String key = content.isEmpty() ? "uncached:" + UUID.randomUUID()
                    : "v41:" + config.identity() + ":" + content + ":" + image.getNVitH() + ":" + image.getNVitW();
            work.add(new Work(key, image));
        }
        return work;
    }

    static MockVitImage.Result compute(EngineRpcService.V41ImageInputPB image, Config config, long maxOutputBytes) throws Exception {
        return simulatedForward(image.getTypesCount(), config, maxOutputBytes,
                MessageDigest.getInstance("SHA-256").digest(image.getPatches().getBf16Data().toByteArray()));
    }

    private static MockVitImage.Result simulatedForward(int rows, Config config, long maxOutputBytes, byte[] digest) {
        long bytes = (long)rows * config.hidden() * 2;
        if (bytes > maxOutputBytes || bytes > Integer.MAX_VALUE)
            throw new IllegalArgumentException("V4.1 embedding exceeds Mock output limit");
        // Simulate model forward using patch content; preserve canonical IDs at P.
        long state = ByteBuffer.wrap(digest).getLong();
        ByteBuffer features = ByteBuffer.allocate((int)bytes).order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer hashes = ByteBuffer.allocate(rows * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int row = 0; row < rows; row++) {
            int start = features.position();
            for (int col = 0; col < config.hidden(); col++) {
                state = MockFeatureHash.mix(state + 0x9e3779b97f4a7c15L);
                float value = (state >>> 40) / (float)(1 << 24);
                features.putShort((short)(Float.floatToIntBits(value) >>> 16));
            }
            hashes.putInt(MockFeatureHash.row(features.array(), start, config.hidden() * 2));
        }
        return new MockVitImage.Result(EngineRpcService.TensorPB.newBuilder()
                .setDataType(EngineRpcService.TensorPB.DataType.BF16).addShape(rows).addShape(config.hidden())
                .setBf16Data(ByteString.copyFrom(features.array())).build(), EngineRpcService.TensorPB.newBuilder()
                .setDataType(EngineRpcService.TensorPB.DataType.INT32).addShape(rows)
                .setInt32Data(ByteString.copyFrom(hashes.array())).build(), rows);
    }
}
