#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

while true
do
  clear
  echo -e "${GREEN}"
  echo "=========================================="
  echo "    🚀 Fertilizantes - Dashboard Web 🚀    "
  echo "=========================================="
  echo -e "${NC}"

  echo -e "${GREEN}1) Iniciar servidor${NC}"
  echo -e "${RED}2) Detener servidor${NC}"
  echo -e "${BLUE}3) Salir${NC}"
  echo ""
  read -p "Selecciona una opción [1-3]: " opcion

  case $opcion in
    1)
      echo -e "${GREEN}Verificando si el puerto 8000 está en uso...${NC}"
      PID=$(lsof -ti :8000)
      if [ -n "$PID" ]; then
        echo -e "${RED}⚠️  Ya hay un servidor corriendo en el puerto 8000 (PID $PID).${NC}"
        read -p "¿Deseas detenerlo? (s/n): " respuesta
        if [ "$respuesta" = "s" ]; then
          kill -9 $PID
          echo -e "${RED}Servidor detenido exitosamente 🛑${NC}"
          sleep 2
        else
          echo -e "${BLUE}Se intentará iniciar de todos modos, pero podría fallar.${NC}"
        fi
      fi

      # ✅ Preguntar si queremos exponer a la red local (para Jaqui)
      read -p "¿Permitir acceso desde la red local (para Jaqui)? [s/n]: " permitir
      if [ "$permitir" = "s" ]; then
        BIND_ADDR="0.0.0.0"
        # Detectar IP local (Wi-Fi suele ser en0; si no, intentar en1)
        IP=$(ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null)
        if [ -z "$IP" ]; then
          IP="(IP_no_detectada)"
        fi
        SHARE_URL="http://${IP}:8000"
        OPEN_URL="http://127.0.0.1:8000"
        EXTERNAL_MSG="Comparte con Jaqui: ${YELLOW}${SHARE_URL}${NC}"
      else
        BIND_ADDR="127.0.0.1"
        OPEN_URL="http://127.0.0.1:8000"
        SHARE_URL=""
        EXTERNAL_MSG=""
      fi

      echo -e "${GREEN}Iniciando servidor Django...${NC}"
      cd /Users/Arturo/AGRICULTURA/FERTILIZANTES || { echo -e "${RED}No se encontró la ruta del proyecto.${NC}"; read -p "Enter para continuar"; continue; }
      # activar venv
      source /Users/Arturo/AGRICULTURA/FERTILIZANTES/ENTORNO/env/bin/activate

      # Iniciar servidor en segundo plano (guardar logs para diagnóstico)
      (python manage.py runserver ${BIND_ADDR}:8000 > server_output.log 2>&1 &)
      sleep 7

      # Verificar si realmente hay algo escuchando en el puerto 8000
      if lsof -nP -iTCP:8000 -sTCP:LISTEN > /dev/null; then
          echo -e "${GREEN}Servidor iniciado correctamente 🚀${NC}"
          # Abrir en Chrome como app la URL local (para tu equipo)
          open -na "Google Chrome" --args --app="${OPEN_URL}"

          # Si hay URL para compartir, mostrarla y copiarla al portapapeles
          if [ -n "$SHARE_URL" ]; then
            echo -e "${EXTERNAL_MSG}"
            echo -n "$SHARE_URL" | pbcopy
            echo -e "${BLUE}(La URL también se copió al portapapeles)${NC}"
          fi
      else
          echo -e "${RED}❌ Hubo un problema iniciando el servidor.${NC}"
          echo -e "${YELLOW}Revisa el archivo server_output.log para más detalles.${NC}"
          echo ""
          read -p "¿Deseas intentar iniciar de nuevo? (s/n): " reintentar
          if [ "$reintentar" = "s" ]; then
              continue
          fi
      fi
      sleep 5
      ;;
    2)
      echo -e "${RED}Deteniendo servidor Django...${NC}"
      PID=$(lsof -ti :8000)
      if [ -n "$PID" ]; then
        kill -9 $PID
        echo -e "${RED}Servidor detenido exitosamente 🛑${NC}"
      else
        echo -e "${BLUE}No hay servidor corriendo actualmente.${NC}"
      fi
      sleep 3
      ;;
    3)
      echo -e "${BLUE}Saliendo... 👋${NC}"
      rm -f server_output.log
      exit 0
      ;;
    *)
      echo -e "${RED}Opción no válida. Intenta de nuevo.${NC}"
      sleep 2
      ;;
  esac
done