import re

def safe_filename(name: str, replacement: str = "_") -> str:
    """
    Remove characters that are invalid for filenames across Windows, macOS, and Linux.
    """
    # Characters not allowed in Windows filenames: \ / : * ? " < > |
    # Also remove control characters and strip whitespace at ends
    return re.sub(r'[<>:"/\\|?*\x00-\x1F]', replacement, name).strip()