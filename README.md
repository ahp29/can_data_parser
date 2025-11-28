CAN Log Parser with DBC Support

This tool parses CAN (Controller Area Network) log files using a DBC (CAN database) file
to extract and visualize signal timeseries data.

USAGE:
    python CAN_parser.py --dbc <path_to_dbc> --log <path_to_log> [OPTIONS]

REQUIRED ARGUMENTS:
    --dbc PATH          Path to the DBC file (database definition)
    --log PATH          Path to the CAN log file (CSV, text, or BusMaster format)

OPTIONAL ARGUMENTS:
    --signals NAMES     Comma-separated list of signal names to extract (e.g., "Speed,Temperature")
                        If not provided, interactive selection will be shown
    --excel PATH        Export collected timeseries data to Excel file with charts
    --html PATH         Export collected timeseries data to interactive HTML file (requires plotly)
    --verbose           Enable verbose logging for debugging

SUPPORTED LOG FORMATS:
    - BusMaster-style: "14:38:10:7213 Rx 1 0x18F0F4F9 x 8 4F 01 00 00 18 08 04 00"
    - Simple formats with: timestamp, CAN ID (hex or decimal), and data bytes

EXAMPLES:

    1. Interactive mode (choose signals from menu):
       python CAN_parser.py --dbc database.dbc --log capture.log

    2. Extract specific signals:
       python CAN_parser.py --dbc database.dbc --log capture.log --signals "EngineSpeed,Throttle"

    3. Export to Excel with charts:
       python CAN_parser.py --dbc database.dbc --log capture.log --signals "Speed" --excel output.xlsx

    4. Export to interactive HTML plot:
       python CAN_parser.py --dbc database.dbc --log capture.log --signals "Speed,RPM" --html plot.html

    5. Extract all signals and export:
       python CAN_parser.py --dbc database.dbc --log capture.log --signals "all" --excel results.xlsx

REQUIREMENTS:
    pip install cantools
    pip install pandas xlsxwriter  # For Excel export
    pip install plotly             # For HTML export
