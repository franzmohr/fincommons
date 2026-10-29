

# You can implement internal corporate design patterns here

linewidth_corporate_design <- .9

# General theme
theme_corporate_design <- function(...) {
  result <- ggplot2::theme(
    axis.title = ggplot2::element_blank(),
    legend.title = ggplot2::element_blank(),
    legend.position = "bottom",
    plot.caption = ggplot2::element_text(hjust = 0),
    ...)
  return(result)
}

# Colour palettes
colours_corporate_design <- c("#EC6600", "#C00808", "#007631",
                              "#005CAA", "#681E79", "#000000",
                              "#008CA5", "#B44B7F", "#6E3C0D")
