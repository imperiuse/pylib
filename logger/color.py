# Класс Цвета ANSII
class Color:

    # Escape sequence	Text attributes
    Off = "\x1b[0m"  # All attributes off(color at startup)
    Bold = "\x1b[1m"  # Bold on(enable foreground intensity)
    Underline = "\x1b[4m"  # Underline on
    Blink = "\x1b[5m"  # Blink on(enable background intensity)
    Bold_off = "\x1b[21m"  # Bold off(disable foreground intensity)
    Underline_off = "\x1b[24m"  # Underline off
    Blink_off = "\x1b[25m"  # Blink off(disable background intensity)

    Black = "\x1b[30m"  # Black
    Red = "\x1b[31m"  # Red
    Green = "\x1b[32m"  # Green
    Yellow = "\x1b[33m"  # Yellow
    Blue = "\x1b[34m"  # Blue
    Magenta = "\x1b[35m"  # Magenta
    Cyan = "\x1b[36m"  # Cyan
    White = "\x1b[37m"  # White
    Default = "\x1b[39m"  # Default(foreground color at startup)
    Light_Gray = "\x1b[90m"  # Light Gray
    Light_Red = "\x1b[91m"  # Light Red
    Light_Green = "\x1b[92m"  # Light Green
    Light_Yellow = "\x1b[93m"  # Light Yellow
    Light_Blue = "\x1b[94m"  # Light Blue
    Light_Magenta = "\x1b[95m"  # Light Magenta
    Light_Cyan = "\x1b[96m"  # Light Cyan
    Light_White = "\x1b[97m"  # Light White
    Reset = "\x1b[0m"

    # "\x1b[40m"   # Black
    # "\x1b[41m"   # Red
    # "\x1b[42m"   # Green
    # "\x1b[43m"   # Yellow
    # "\x1b[44m"   # Blue
    # "\x1b[45m"   # Magenta
    # "\x1b[46m"   # Cyan
    # "\x1b[47m"   # White
    # "\x1b[49m"   # Default(background color at startup)
    # "\x1b[100m"  # Light Gray
    # "\x1b[101m"  # Light Red
    # "\x1b[102m"  # Light Green
    # "\x1b[103m"  # Light Yellow
    # "\x1b[104m"  # Light Blue
    # "\x1b[105m"  # Light Magenta
    # "\x1b[106m"  # Light Cyan
    # "\x1b[107m"  # Light White


