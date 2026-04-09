"""Data processing and transformations"""
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType
from image_utils import letterbox_resize_image


def build_bboxes(df_labels_parsed):
    """Group bounding boxes by image"""
    df_bboxes = (
        df_labels_parsed
        # Combine coordinates into an array
        .withColumn("reg_target", F.array("cx", "cy", "w", "h"))
        .withColumn("pos_mask", F.lit(1.0))
        .groupBy("image_name")
        # Collect lists of targets for each image
        .agg(
            F.collect_list("class_id").alias("cls_targets"),
            F.collect_list("reg_target").alias("reg_targets"),
            F.collect_list("pos_mask").alias("pos_mask")
        )
    )
    return df_bboxes


def join_data(df_images, df_bboxes):
    """Join images and labels"""
    print("Joining images and labels...")

    df = df_images.join(df_bboxes, on="image_name", how="left")

    # Handle null values for images without labels
    df = (
        df
        .withColumn("cls_targets", F.when(F.col("cls_targets").isNull(), F.array()).otherwise(F.col("cls_targets")))
        .withColumn("reg_targets", F.when(F.col("reg_targets").isNull(), F.array()).otherwise(F.col("reg_targets")))
        .withColumn("pos_mask", F.when(F.col("pos_mask").isNull(), F.array()).otherwise(F.col("pos_mask")))
    )

    return df


# UDF to update labels after letterbox resize
@F.udf(ArrayType(ArrayType(FloatType())))
def transform_labels_letterbox(reg_targets, scale, pad_x, pad_y):
    if reg_targets is None or scale is None:
        return []

    transformed = []
    # Calculate image area within the letterbox
    img_region_w = 1.0 - 2.0 * pad_x
    img_region_h = 1.0 - 2.0 * pad_y

    for bbox in reg_targets:
        if bbox is None or len(bbox) < 4:
            continue

        cx_orig, cy_orig, w_orig, h_orig = bbox[0], bbox[1], bbox[2], bbox[3]

        # Shift and scale coordinates
        cx_new = cx_orig * img_region_w + pad_x
        cy_new = cy_orig * img_region_h + pad_y
        w_new = w_orig * img_region_w
        h_new = h_orig * img_region_h

        transformed.append([float(cx_new), float(cy_new), float(w_new), float(h_new)])

    return transformed


def resize_images(df):
    """Letterbox resize images and update labels"""
    print("Letterbox resizing images...")

    # Run resize + get transformation parameters
    df = df.withColumn("letterbox_result", letterbox_resize_image("raw_content"))

    # Extract image data and padding info
    df = (
        df
        .withColumn("images", F.col("letterbox_result.image"))
        .withColumn("lb_scale", F.col("letterbox_result.scale"))
        .withColumn("lb_pad_x", F.col("letterbox_result.pad_x"))
        .withColumn("lb_pad_y", F.col("letterbox_result.pad_y"))
        .drop("raw_content", "letterbox_result")
    )

    # Remove failed resizes
    df = df.filter(F.col("images").isNotNull())

    # Map labels to new image coordinates
    df = df.withColumn(
        "reg_targets",
        transform_labels_letterbox(
            F.col("reg_targets"),
            F.col("lb_scale"),
            F.col("lb_pad_x"),
            F.col("lb_pad_y")
        )
    )

    # Clean up temp columns
    df = df.drop("lb_scale", "lb_pad_x", "lb_pad_y")

    return df