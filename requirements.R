packages <- c(
  "tidyverse",
  "conflicted",
  "mosaic",
  "ISOweek",
  "r2r"
)

for (package in packages) {
  if (!require(package, character.only = TRUE)) {
    install.packages(package, repos="https://cloud.r-project.org")
  }
}
