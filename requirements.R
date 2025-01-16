packages <- c(
  "ggplot2",
  "dplyr",
  "tidyr"
)

for (package in packages) {
  if (!require(package, character.only = TRUE)) {
    install.packages(package, repos="https://cloud.r-project.org")
  }
}
