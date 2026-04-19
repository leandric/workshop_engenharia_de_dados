#!/bin/sh
set -eu

USER_NAME="teste"
USER_UID="1001"
USER_GID="100"

UPLOAD_BASE="/home/${USER_NAME}/upload"



# Ajusta ownership para o usuário do SFTP conseguir escrever
chown -R "${USER_UID}:${USER_GID}" "${UPLOAD_BASE}"

# Mantém permissões razoáveis para laboratório
chmod -R 755 "${UPLOAD_BASE}"

# Chama o entrypoint original da imagem
exec /entrypoint "$@"