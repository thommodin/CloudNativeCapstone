import h3
import shapely
import typing

def polygon_to_h3_indexes(
    polygon: shapely.geometry.Polygon,
    resolution: int,
    return_dtype: typing.Literal["str", "int"] = "str",
) -> list[str] | list[int]:
    """
    Convert a polygon to a list of H3 indexes at the given resolution.

    Uses H3's experimental `polygon_to_cells_experimental` with overlapping
    containment mode to include any cells that touch the polygon boundary.

    Args:
        polygon: Shapely polygon defining the target region.
        resolution: H3 resolution level (0-15).
        return_dtype: Output type for H3 indexes ("str" hex IDs or "int").

    Returns:
        Sorted list of H3 indexes as strings or integers.
    """

    # Convert the Shapely polygon to an H3 shape.
    h3_shape = h3.geo_to_h3shape(geo=polygon)
    
    # Get H3 cells covering this polygon using experimental function
    # with overlapping containment mode to include cells that overlap the polygon
    h3_cells = h3.polygon_to_cells_experimental(
        h3_shape, 
        resolution,
        contain="bbox_overlap"
    )
    
    match return_dtype:
        case "str":
            return sorted(h3_cells)
        case "int":
            return sorted([int(h3_cell, 16) for h3_cell in h3_cells])
        case _:
            raise NotImplementedError()
