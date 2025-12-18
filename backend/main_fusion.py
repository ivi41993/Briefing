from fastapi import FastAPI
import os

# 1. Importamos todas tus apps (Asegúrate de que los nombres de archivo coinciden)
from main import app as app_madrid
from main_bcn import app as app_bcn
from main_vlc import app as app_vlc
from main_alm import app as app_alm
from main_wfs1 import app as app_wfs1
from main_wfs2 import app as app_wfs2
from main_wfs3 import app as app_wfs3
from main_wfs1alm import app as app_wfs1alm
from main_wfs2alm import app as app_wfs2alm
from main_wfs3alm import app as app_wfs3alm
# Añade aquí si me he dejado alguna...

# 2. Creamos la APP GIGANTE
root_app = FastAPI(title="Planti Fusion System")

# 3. Montamos cada ciudad en su ruta (Igual que hacía Traefik/Nginx, pero ahora lo hace Python)
root_app.mount("/bcn", app_bcn)
root_app.mount("/vlc", app_vlc)
root_app.mount("/alm", app_alm)
root_app.mount("/wfs1", app_wfs1)
root_app.mount("/wfs2", app_wfs2)
root_app.mount("/wfs3", app_wfs3)
root_app.mount("/wfs1alm", app_wfs1alm)
root_app.mount("/wfs2alm", app_wfs2alm)
root_app.mount("/wfs3alm", app_wfs3alm)

# 4. Madrid se queda en la raíz (IMPORTANTE: Montar la raíz siempre la última)
root_app.mount("/", app_madrid)

# Este archivo será el único que ejecutará el servidor
