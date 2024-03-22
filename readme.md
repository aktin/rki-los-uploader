# Preparation

## Install R and R packages "conflicted"," dplyr", "readr", "tidyverse", "lubridate", "mosaic", "ISOweek", "r2r"
Set R path in PATH VARIABLES of the system, Here is a example for a python 
method that does that for you: 

```
def set_path_variable(path_toml: str) -> None:
    """
    This Method sets the path variable in an Windows environment. This is necessary for executing the Rscript for
    Length of stay.
    :return:
    """
    config = toml.load(path_toml)
    # Specify the directory containing Rscript.exe
    r_bin_dir = os.environ['RSCRIPT.R_DIR']

    # Get the current value of the PATH environment variable
    current_path = os.environ.get('PATH', '')

    # Append the R bin directory to the PATH, separating it with the appropriate separator
    new_path = f"{current_path};{r_bin_dir}" if current_path else r_bin_dir

    # Update the PATH environment variable
    os.environ['PATH'] = new_path
```
Linux(Terminal): export PATH=$PATH:/usr/lib/R




