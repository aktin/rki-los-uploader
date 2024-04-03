# Preparation
You need:
1. a distribution of R (we used 4.1.2)


## Install R and R packages "conflicted"," dplyr", "readr", "tidyverse", "lubridate", "mosaic", "ISOweek", "r2r"
First install a distribution of R (we used 4.1.2).
Set R path in PATH VARIABLES of the system. For Linux, paste the following command in the Terminal. Normally the R executable should be installed to the given 
directory, if not change the path: 

***Set PATH to R in Linux*** 

```export PATH=$PATH:/usr/lib/R```

### Trouble installing R packages
If you encounter Problems installing R packages with 'install.package', use this comand in the terminal and try to install again:
```sudo apt install libssl-dev libcurl4-openssl-dev unixodbc-dev libxml2-dev libmariadb-dev libfontconfig1-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev```

## Configuration TOML
Create a toml file 

| Header | Attribute | Description                  |
|--------|-----------|------------------------------|
| BROKER | URL       | Literal path to AKTIN broker |
|        | API_KEY   | Authorisation key for broker |
| Cell 7 | Cell 8    | Cell 9                       |



