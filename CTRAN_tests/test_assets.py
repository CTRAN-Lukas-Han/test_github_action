from os import getenv
from dagster import materialize, Definitions, load_assets_from_modules
from CTRAN import *
from CTRAN.assets import *
from CTRAN.resources import INITResource, INITDB, init_resource

init_resource_instance = INITResource(
    username=os.getenv("INIT_USERNAME"),
    password=os.getenv("INIT_PASSWORD"),
    connection_string="//INIT-CTRAN-ORC.c-tran.org:1521/MOBILE.INIT-CTRAN-ORC"
)

def test_assets():
    assets = [health_check_asset, my_asset]
    result = materialize(assets, resources={'INIT': init_resource_instance})
    assert result.success

