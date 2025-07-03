import os
from contextlib import suppress
from copy import deepcopy
from pathlib import Path
from traceback import print_exc

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

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


@register()
def serve_vite_app(app: FastAPI):
    """Serve Vite app at the root path with SPA routing support."""
    
    from universql.util import current_context
    
    # Check if frontend directory is specified in context
    frontend_dir_from_context = current_context.get('frontend_dir')
    
    if frontend_dir_from_context:
        # Use specified frontend directory
        static_dir = Path(frontend_dir_from_context)
        index_file = static_dir / "index.html"
        
        if not static_dir.exists():
            raise ValueError(f"Specified frontend directory not found: {static_dir}")
        elif not index_file.exists():
            raise ValueError(f"index.html not found in specified directory: {static_dir}")
    else:
        raise ValueError("No frontend directory specified in context")
    
    
    # Mount static files for assets (CSS, JS, images, etc.) if assets directory exists
    assets_dir = static_dir / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
        print(f"📁 Serving assets from {assets_dir}")
    else:
        print(f"⚠️  Assets directory not found at {assets_dir}, skipping assets mount")
    
    # Handle favicon and other root-level static files
    @app.get("/favicon.ico")
    async def favicon():
        favicon_path = static_dir / "favicon.ico"
        if favicon_path.exists():
            return FileResponse(favicon_path)
        return FileResponse(static_dir / "vite.svg")  # Fallback to Vite default
    
    # Handle other static files that might be in the root
    static_extensions = {".ico", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".txt", ".xml", ".json"}
    
    @app.get("/{filename:path}")
    async def serve_static_or_spa(filename: str):
        file_path = static_dir / filename
        
        # If it's a static file and exists, serve it
        if file_path.exists() and file_path.suffix.lower() in static_extensions:
            return FileResponse(file_path)
        
        # For API routes, let FastAPI handle them normally
        if filename.startswith(("api/", "ws", "health", "db/")):
            # This will be handled by other routes
            pass
        else:
            # For all other routes (SPA routing), serve index.html
            return FileResponse(index_file, media_type="text/html")
    
    # Catch-all route for SPA routing (must be last)
    @app.get("/")
    async def serve_spa_root():
        return FileResponse(index_file, media_type="text/html")

