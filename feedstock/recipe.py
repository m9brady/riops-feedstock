from datetime import datetime, date, timedelta
from os.path import basename
from pangeo_forge_recipes.patterns import ConcatDim, MergeDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# RIOPS is run every 6 hours (at 00, 06, 12, and 18 UTC).
# but, MSC usually only has 2-days-worth of data on the datamart.
# Thus, we use yesterday's date to ensure the feedstock can run
def make_url(variable, time):
    """Currently hardcoded for 2D variables run at 00Z"""
    riops_ps_base_url = (
        "https://dd.weather.gc.ca/model_riops/netcdf/forecast/polar_stereographic"
    )
    start_date = datetime.combine(date.today() - timedelta(days=1), datetime.min.time())
    yyyymmdd = start_date.strftime("%Y%m%d")
    fh = f"{time:03.0f}"
    # vertical level (SFC or DBS-0.5m for 2D, DBS-all for 3D)
    if variable in ["VOTEMPER", "VOSALINE", "VOZOCRTX", "VOMECRTY"]:
        level = "DBS-0.5m"  # Depth-Below-Surface 0.5m
    else:
        level = "SFC"  # Surface
    url = f"{riops_ps_base_url}/2d/00/{fh}/{yyyymmdd}T00Z_MSC_RIOPS_{variable}_{level}_PS5km_P{fh}.nc"
    return url


# placeholders for other 2D variables
variable_merge_dim = MergeDim(
    "variable",
    [
        "VOTEMPER",  # Potential temperature of sea water
        "VOSALINE",  # Sea water salinity
        "VOZOCRTX",  # X-component of water velocity
        "VOMECRTY",  # Y-component of water velocity
        # "IICECONC",  # Sea ice fraction
        # "IICEVOL",  # Volume of sea ice per unit grid cell area
        # "ISNOWVOL",  # Volume of snow on sea ice per unit grid cell area
        # "IICESTRENGTH",  # Vertically integrated compressive ice strength
        # "IICEPRESSURE",  # Vertically integrated ice internal pressure
        # "IICESURFTEMP",  # Surface temperature of sea ice/snow
        # "IICEDIVERGENCE",  # Sea ice divergence
        # "IICESHEAR",  # Sea ice shear
        # "ITZOCRTX",  # X-component of sea ice velocity
        # "ITMERCTY",  # Y-component of sea ice velocity
        # "SOKARAML",  # Mixed layer depth based on a density criterion
        # "SOMIXHGT",  # Turbocline depth
        # "SOSSHEIG",  # Sea surface height
    ],
)

# A RIOPS forecast is every hour for 84 hours
time_concat_dim = ConcatDim("time", range(84), nitems_per_file=1)

pattern = FilePattern(make_url, variable_merge_dim, time_concat_dim)


def process_input(ds, filename):
    # use the first 8 chars of filename to get the model run date
    model_run_date = datetime.strptime(basename(filename)[:8], "%Y%m%d")
    ds = ds.drop("polar_stereographic")
    # use an encoding that is valid of hourly data
    units = f'hours since {model_run_date.strftime("%Y-%m-%d")} 00:00:00'
    ds.time.encoding = {"units": units, "calendar": "proleptic_gregorian"}
    return ds


recipe = XarrayZarrRecipe(
    file_pattern=pattern,
    target_chunks={"time": 1, "xc": 450, "yc": 410},
    process_input=process_input,
)
