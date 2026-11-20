"""Publica y descarga archivos del share SMB usado como DWH de archivos del proyecto.

Mismo patrón que ya se usa en otros DAGs (conexión de Airflow + smbclient, con
verificación por hash al subir). Aquí se generaliza para cualquier archivo, no
solo reportes XLSX, y se agrega la operación inversa (descarga) porque las
capas Bronze/Silver necesitan poder leerse de vuelta en la tarea siguiente.
"""
from __future__ import annotations

from hashlib import sha256
from pathlib import Path, PureWindowsPath
from shutil import copyfileobj

CONN_ID = "dwh_smb_reports"
SHARE = "reportes"
PROYECTO = "co2_percapita_etl"


def _huella(stream) -> bytes:
    resumen = sha256()
    for bloque in iter(lambda: stream.read(1024 * 1024), b""):
        resumen.update(bloque)
    return resumen.digest()


def _validar_carpeta(carpeta: str) -> PureWindowsPath:
    ruta = PureWindowsPath(str(carpeta).replace("/", "\\"))
    if ruta.is_absolute() or ".." in ruta.parts or ":" in str(ruta):
        raise ValueError("La carpeta SMB debe ser relativa al share de la conexión.")
    return ruta


def _conexion():
    try:
        from airflow.sdk import BaseHook
    except ImportError:  # Airflow 2
        from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(CONN_ID)
    if not conn.host or not conn.login or not conn.password:
        raise ValueError(f"La conexión {CONN_ID} debe tener host, usuario y contraseña.")
    return conn


def _registrar_sesion(conn) -> int:
    import smbclient

    port = conn.port or 445
    smbclient.register_session(
        server=conn.host,
        username=conn.login,
        password=conn.password,
        port=port,
        connection_timeout=15,
    )
    return port


def subir_archivo(archivo_local: Path, carpeta: str) -> str:
    """Sube un archivo local al share SMB y verifica la copia con un hash.

    `carpeta` es relativa a la raíz del share (ej. "co2_percapita_etl/bronze/owid").
    Devuelve la ruta SMB completa del archivo subido.
    """
    import smbclient

    archivo_local = Path(archivo_local)
    if not archivo_local.is_file():
        raise ValueError(f"No existe el archivo local: {archivo_local}")
    carpeta = _validar_carpeta(carpeta)

    conn = _conexion()
    port = _registrar_sesion(conn)

    raiz = f"\\\\{conn.host}\\{SHARE}"
    directorio = f"{raiz}\\{carpeta}"
    destino = f"{directorio}\\{archivo_local.name}"

    smbclient.makedirs(directorio, exist_ok=True, port=port)
    with archivo_local.open("rb") as origen, smbclient.open_file(destino, mode="wb", port=port) as remoto:
        copyfileobj(origen, remoto)
        origen.seek(0)
        huella_local = _huella(origen)
    with smbclient.open_file(destino, mode="rb", port=port) as remoto:
        if _huella(remoto) != huella_local:
            raise RuntimeError(f"El archivo leído de SMB no coincide con {archivo_local}.")
    return destino


def descargar_archivo(ruta_smb: str, destino_local: Path) -> Path:
    """Descarga un archivo del share SMB a una ruta local (la crea si no existe)."""
    import smbclient

    conn = _conexion()
    port = _registrar_sesion(conn)

    destino_local = Path(destino_local)
    destino_local.parent.mkdir(parents=True, exist_ok=True)
    with smbclient.open_file(ruta_smb, mode="rb", port=port) as remoto:
        with destino_local.open("wb") as local:
            copyfileobj(remoto, local)
    return destino_local


def nombre_archivo(ruta_smb: str) -> str:
    """Nombre del archivo a partir de una ruta SMB (siempre con `\\`, sin importar el OS)."""
    return PureWindowsPath(ruta_smb).name
