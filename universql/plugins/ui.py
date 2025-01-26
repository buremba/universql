from contextlib import suppress
from copy import deepcopy
from traceback import print_exc

from fastapi import FastAPI

from universql.plugin import register


# @register()
def run_marimo(app: FastAPI):
    with suppress(ImportError):
        import marimo
        app.mount("/", marimo.create_asgi_app(include_code=False)
                  .with_dynamic_directory(path="/", directory="/Users/bkabak/.universql/marimos")
                  .build())

def configure(components, config):
    # TODO: generalize to arbitrary nested dictionaries, not just one level
    _components = deepcopy(components)
    for k1, v1 in config.items():
        for k2, v2 in v1.items():
            _components[k1][k2] = v2
    return _components


# @register()
async def run_jupyter(app: FastAPI):
    try:
        from asphalt.core import Context
        from jupyverse_api.main import JupyverseComponent
        from jupyverse_api.app import App

        components = configure({"app": {"type": "app"}}, {"app": {"mount_path": '/notebook'}})
        async with Context() as ctx:
            component = JupyverseComponent(components=components, app=app, debug=True)
            component.start(ctx)
            jupyter_app = await ctx.request_resource(App)
            app.mount("/notebook", jupyter_app)
    except Exception as e:
        print_exc(10)
        print(e)

