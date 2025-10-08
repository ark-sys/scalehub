from unittest.mock import patch, MagicMock

import pytest

from scripts.utils.Logger import Logger, Color


class TestLogger:
    """Test suite for the Logger class."""

    @pytest.fixture
    def logger(self):
        """Create a fresh Logger instance for each test."""
        return Logger()

    @pytest.fixture
    def mock_datetime(self):
        """Mock datetime.now() to return a predictable timestamp."""
        with patch("scripts.utils.Logger.datetime") as mock_dt:
            mock_datetime_instance = MagicMock()
            mock_datetime_instance.isoformat.return_value = "2024-01-01T12:00:00"
            mock_dt.now.return_value = mock_datetime_instance
            yield mock_dt

    def test_logger_initialization_default_values(self, logger):
        """Test Logger initializes with correct default values."""
        assert logger.info_color == Color.dull_white
        assert logger.warning_color == Color.yellow
        assert logger.error_color == Color.pure_red
        assert logger.debug_color == Color.light_cyan
        assert logger.reset_color == Color.reset_color
        assert logger.debug_level == 0

    def test_date_time_format(self, mock_datetime):
        """Test date_time() returns correctly formatted timestamp."""
        result = Logger.date_time()
        assert result == "[2024-01-01T12:00:00]"

    def test_new_line_prints_empty_line(self):
        """Test new_line() prints an empty line."""
        with patch("builtins.print") as mock_print:
            Logger.new_line()
            mock_print.assert_called_once_with()

    def test_info_message_format(self, logger, mock_datetime):
        """Test info() prints message with correct format."""
        with patch("builtins.print") as mock_print:
            logger.info("Test message")
            expected = f"{Color.reset_color}[2024-01-01T12:00:00] Test message"
            mock_print.assert_called_once_with(expected)

    def test_info_message_with_kwargs(self, logger, mock_datetime):
        """Test info() passes kwargs to print function."""
        with patch("builtins.print") as mock_print:
            logger.info("Test message", end="", flush=True)
            expected = f"{Color.reset_color}[2024-01-01T12:00:00] Test message"
            mock_print.assert_called_once_with(expected, end="", flush=True)

    def test_debug_message_level_0_not_printed(self, logger, mock_datetime):
        """Test debug() doesn't print when debug_level is 0."""
        with patch("builtins.print") as mock_print:
            logger.debug("Debug message")
            mock_print.assert_not_called()

    def test_debug_message_level_1_printed(self, logger, mock_datetime):
        """Test debug() prints when debug_level > 0."""
        logger.set_debug_level(1)
        with patch("builtins.print") as mock_print:
            mock_print.reset_mock()
            logger.debug("Debug message")
            expected = f"{Color.light_cyan}[2024-01-01T12:00:00] [DEBUG] + Debug message {Color.reset_color}"
            mock_print.assert_called_once_with(expected)

    def test_debugg_message_level_1_not_printed(self, logger, mock_datetime):
        """Test debugg() doesn't print when debug_level <= 1."""
        logger.set_debug_level(1)
        with patch("builtins.print") as mock_print:
            mock_print.reset_mock()
            logger.debugg("Debug message")
            mock_print.assert_not_called()

    def test_debugg_message_level_2_printed(self, logger, mock_datetime):
        """Test debugg() prints when debug_level > 1."""
        logger.set_debug_level(2)
        with patch("builtins.print") as mock_print:
            mock_print.reset_mock()
            logger.debugg("Debug message")
            expected = f"{Color.light_cyan}[2024-01-01T12:00:00] [DEBUG] ++ Debug message {Color.reset_color} "
            mock_print.assert_called_once_with(expected)

    def test_debuggg_message_level_2_not_printed(self, logger, mock_datetime):
        """Test debuggg() doesn't print when debug_level <= 2."""
        logger.set_debug_level(2)
        with patch("builtins.print") as mock_print:
            mock_print.reset_mock()
            logger.debuggg("Debug message")
            mock_print.assert_not_called()

    def test_debuggg_message_level_3_printed(self, logger, mock_datetime):
        """Test debuggg() prints when debug_level > 2."""
        logger.set_debug_level(3)
        with patch("builtins.print") as mock_print:
            mock_print.reset_mock()
            logger.debuggg("Debug message")
            expected = f"{Color.light_cyan}[2024-01-01T12:00:00] [DEBUG] +++ Debug message {Color.reset_color} "
            mock_print.assert_called_once_with(expected)

    def test_warning_message_format(self, logger, mock_datetime):
        """Test warning() prints message with correct format."""
        with patch("builtins.print") as mock_print:
            logger.warning("Warning message")
            expected = f"{Color.yellow}[2024-01-01T12:00:00] [WARNING] Warning message{Color.reset_color}"
            mock_print.assert_called_once_with(expected)

    def test_error_message_format(self, logger, mock_datetime):
        """Test error() prints message with correct format."""
        with patch("builtins.print") as mock_print:
            logger.error("Error message")
            expected = f"{Color.pure_red}[2024-01-01T12:00:00] [ERROR] Error message{Color.reset_color}"
            mock_print.assert_called_once_with(expected)

    def test_set_debug_level_updates_level(self, logger):
        """Test set_debug_level() updates debug_level and prints info message."""
        with patch("builtins.print") as mock_print:
            logger.set_debug_level(2)
            assert logger.debug_level == 2
            assert mock_print.call_count == 1

    @pytest.mark.parametrize("level", [0, 1, 2, 3, 5])
    def test_debug_levels_behavior(self, logger, mock_datetime, level):
        """Test debug methods behavior at different debug levels."""
        logger.set_debug_level(level)

        with patch("builtins.print") as mock_print:
            mock_print.reset_mock()

            logger.debug("test")
            logger.debugg("test")
            logger.debuggg("test")

            expected_calls = 0
            if level > 0:
                expected_calls += 1
            if level > 1:
                expected_calls += 1
            if level > 2:
                expected_calls += 1

            assert mock_print.call_count == expected_calls

    def test_thousands_formatter_static_method(self):
        """Test thousands_formatter() static method returns correct format."""
        result = Logger.thousands_formatter(1000, 0)
        assert result == "1k"

        result = Logger.thousands_formatter(5500, 0)
        assert result == "6k"

        result = Logger.thousands_formatter(500, 0)
        assert result == "0k"

        result = Logger.thousands_formatter(1500, 0)
        assert result == "2k"

    def test_all_log_methods_accept_kwargs(self, logger, mock_datetime):
        """Test all logging methods accept and pass through kwargs."""
        with patch("builtins.print") as mock_print:
            logger.set_debug_level(3)
            mock_print.reset_mock()

            test_kwargs = {"end": "\n", "flush": True}

            logger.info("test", **test_kwargs)
            logger.warning("test", **test_kwargs)
            logger.error("test", **test_kwargs)
            logger.debug("test", **test_kwargs)
            logger.debugg("test", **test_kwargs)
            logger.debuggg("test", **test_kwargs)

            for call in mock_print.call_args_list:
                assert call.kwargs == test_kwargs


class TestColor:
    """Test suite for the Color class constants."""

    def test_color_constants_defined(self):
        """Test that all expected color constants are defined."""
        expected_colors = [
            "pure_red",
            "dark_green",
            "orange",
            "dark_blue",
            "bright_purple",
            "dark_cyan",
            "dull_white",
            "pure_black",
            "bright_red",
            "light_green",
            "yellow",
            "bright_blue",
            "magenta",
            "light_cyan",
            "bright_black",
            "bright_white",
            "cyan_back",
            "purple_back",
            "white_back",
            "blue_back",
            "orange_back",
            "green_back",
            "pink_back",
            "grey_back",
            "grey",
            "bold",
            "underline",
            "italic",
            "darken",
            "invisible",
            "reverse_colour",
            "reset_color",
        ]

        for color in expected_colors:
            assert hasattr(Color, color)
            assert isinstance(getattr(Color, color), str)
            assert getattr(Color, color).startswith("\033")

    def test_reset_color_value(self):
        """Test reset_color has correct ANSI sequence."""
        assert Color.reset_color == "\033[0m"
