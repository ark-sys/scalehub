# Copyright (C) 2025 Khaled Arsalane
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from datetime import datetime


class Color:
    pure_red = "\033[0;31m"
    dark_green = "\033[0;32m"
    orange = "\033[0;33m"
    dark_blue = "\033[0;34m"
    bright_purple = "\033[0;35m"
    dark_cyan = "\033[0;36m"
    dull_white = "\033[0;37m"
    pure_black = "\033[0;30m"
    bright_red = "\033[0;91m"
    light_green = "\033[0;92m"
    yellow = "\033[0;93m"
    bright_blue = "\033[0;94m"
    magenta = "\033[0;95m"
    light_cyan = "\033[0;96m"
    bright_black = "\033[0;90m"
    bright_white = "\033[0;97m"
    cyan_back = "\033[0;46m"
    purple_back = "\033[0;45m"
    white_back = "\033[0;47m"
    blue_back = "\033[0;44m"
    orange_back = "\033[0;43m"
    green_back = "\033[0;42m"
    pink_back = "\033[0;41m"
    grey_back = "\033[0;40m"
    grey = "\033[38;4;236m"

    bold = "\033[1m"
    underline = "\033[4m"
    italic = "\033[3m"

    darken = "\033[2m"
    invisible = "\033[08m"
    reverse_colour = "\033[07m"
    reset_color = "\033[0m"


class Logger:
    def __init__(self):
        self.info_color = Color.dull_white
        self.warning_color = Color.yellow
        self.error_color = Color.pure_red
        self.debug_color = Color.light_cyan
        self.reset_color = Color.reset_color

        self.debug_level = 0

    @staticmethod
    def new_line():
        print()

    @staticmethod
    def date_time() -> str:
        return "[" + datetime.now().isoformat() + "]"

    def info(self, message: str, **kwargs) -> None:
        print(self.reset_color + f"{self.date_time()} {message}", **kwargs)

    def debug(self, message: str, **kwargs) -> None:
        if self.debug_level > 0:
            print(
                f"{self.debug_color}{self.date_time()} [DEBUG] + {message} {self.reset_color}",
                **kwargs,
            )

    def debugg(self, message: str, **kwargs) -> None:
        if self.debug_level > 1:
            print(
                f"{self.debug_color}{self.date_time()} [DEBUG] ++ {message} {self.reset_color} ",
                **kwargs,
            )

    def debuggg(self, message: str, **kwargs) -> None:
        if self.debug_level > 2:
            print(
                f"{self.debug_color}{self.date_time()} [DEBUG] +++ {message} {self.reset_color} ",
                **kwargs,
            )

    def warning(self, message: str, **kwargs) -> None:
        print(
            f"{self.warning_color}{self.date_time()} [WARNING] {message}{self.reset_color}",
            **kwargs,
        )

    def error(self, message: str, **kwargs) -> None:
        print(
            f"{self.error_color}{self.date_time()} [ERROR] {message}{self.reset_color}",
            **kwargs,
        )

    def set_debug_level(self, new_level):
        self.debug_level = new_level
        self.info(f"[LOGGER] New debug level: {self.debug_level}")

    @staticmethod
    def thousands_formatter(x, pos):
        # The two args are the value and tick position
        return "%1.0fk" % (x * 1e-3)
