"""Decode formats missing from ImageIO with the real image loader's RGB semantics."""
import io
import struct
import sys

from PIL import Image, ImageFile

try:
    import pillow_avif  # noqa: F401
except ImportError:
    pass
try:
    from pillow_heif import register_heif_opener

    register_heif_opener()
except ImportError:
    pass

# Match ImageEmbeddingInterface while retaining the Mock decoded-pixel cap.
ImageFile.LOAD_TRUNCATED_IMAGES = True
try:
    with Image.open(io.BytesIO(sys.stdin.buffer.read())) as source:
        width, height = source.size
        if width <= 0 or height <= 0 or width * height > int(sys.argv[1]):
            sys.exit(3)
        pixels = source.convert("RGB").tobytes()
    sys.stdout.buffer.write(struct.pack(">II", width, height))
    sys.stdout.buffer.write(pixels)
except Exception:
    # Neither input bytes nor URLs belong in subprocess diagnostics.
    sys.exit(2)
