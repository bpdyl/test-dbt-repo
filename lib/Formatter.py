"""##################################################################
# Script      : Formatter.py
# Description : This module has function for formatting values in correct numeric or business type
# Modifications
# 24/01/2023  : Robling  : Initial Script
#####################################################################"""

class Formatter:
    # This Class is used to format value in correct numeric or business format
    def __init__(self):
       self.format_identifier = {'money': "${:,.2f}",
                                 'decimal(2)': "{:,.2f}",
                                 'decimal(1)': "{:,.1f}",
                                 'percent': "{:.2f}%",
                                 'unit': "{:,.0f}"
                                 }
       self.format_style = None

    def format(self, value):
        # This function returns value in correct format order

        return self.format_style.format(value)

    def format_dataframe(self, df, format_description):
        print(df)
        # This function takes dataframe and format description as parameter and formats
        # the value of dataframe as per format description and returns the formatted dataframe

        for i, column in enumerate(df.columns):
            self.format_style = self.format_identifier[format_description[i]]
            df[column] = df[column].apply(self.format)
        return df

    def format_reconciliation_json(self, json_data):
        """Formats a reconciliation JSON using dynamically mapped formatting rules."""
        def apply_format(value, fmt):
            """Apply formatting to a value based on the format description."""
            try:
                return self.format_identifier[fmt].format(value)
            except (KeyError, ValueError):
                return value  # Return original if format not found or invalid
        # Default format description (if missing)
        default_format = ['unit', 'unit', 'unit', 'percent', 'money', 'money', 'money', 'percent', 'money', 'money', 'money', 'percent']
        
        # Extract format description from JSON, or use default
        format_desc = json_data.get('format_description', default_format)
        if isinstance(format_desc, str):
            format_desc = format_desc.split(',')

        # Define the order of keys to process and their corresponding format slices
        key_order = ["unt", "cst", "rtl"]
        for key in key_order:
            if key in json_data["results"]:
                key_index = key_order.index(key)
                start_index = key_index * 4  # Each key uses 4 format descriptors
                col_formats = format_desc[start_index : start_index + 4]
                
                for i, metric in enumerate(["system1_value", "system2_value", "difference", "variance_percent"]):
                    if metric in json_data["results"][key]:
                        json_data["results"][key][metric] = apply_format(
                            json_data["results"][key][metric], col_formats[i]
                        )
        return json_data
