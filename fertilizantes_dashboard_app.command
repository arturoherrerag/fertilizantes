#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Definimos la ruta del log fija para poder encontrarla siempre
PROJECT_DIR="/Users/Arturo/AGRICULTURA/FERTILIZANTES"
LOG_FILE="$PROJECT_DIR/server_output.log"

get_local_ip() {
    local DEFAULT_IF=$(route get default 2>/dev/null | grep interface | awk '{print $2}')
    local DETECTED_IP=""
    if [ -n "$DEFAULT_IF" ]; then
        DETECTED_IP=$(ipconfig getifaddr "$DEFAULT_IF")
    fi
    if [ -z "$DETECTED_IP" ]; then
        DETECTED_IP=$(ifconfig | grep "inet " | grep -v 127.0.0.1 | awk '{print $2}' | head -n 1)
    fi
    echo "$DETECTED_IP"
}

while true
do
  clear
  MY_IP=$(get_local_ip)
  
  echo -e "${GREEN}"
  echo "=========================================="
  echo "    🚀 Fertilizantes - Dashboard Web 🚀    "
  echo "=========================================="
  echo -e "${NC}"
  
  # Estado del servidor
  PID_CHECK=$(lsof -ti :8000)
  if [ -n "$PID_CHECK" ]; then
      echo -e "Estado: ${GREEN}🟢 ACTIVO (PID $PID_CHECK)${NC}"
      echo -e "Acceso: ${YELLOW}http://${MY_IP}:8000${NC}"
  else
      echo -e "Estado: ${RED}🔴 DETENIDO${NC}"
  fi
  echo "------------------------------------------"

  echo -e "${GREEN}1) Iniciar servidor${NC}"
  echo -e "${RED}2) Detener servidor${NC}"
  echo -e "${YELLOW}3) 📂 Ver errores (Abrir Log)${NC}"
  echo -e "${BLUE}4) Salir${NC}"
  echo ""
  read -p "Selecciona una opción [1-4]: " opcion

  case $opcion in
    1)
      echo -e "${GREEN}Verificando puerto 8000...${NC}"
      PID=$(lsof -ti :8000)
    
      if [ -n "$PID" ]; then
        echo -e "${RED}⚠️  El servidor ya está corriendo.${NC}"
        read -p "¿Reiniciarlo? (s/n): " respuesta
        if [ "$respuesta" = "s" ]; then
          kill -9 $PID
          sleep 1
        else
          continue
        fi
      fi

      BIND_ADDR="0.0.0.0"
      IP=$(get_local_ip)
      
      echo -e "${GREEN}Iniciando servidor Django...${NC}"
      
      cd "$PROJECT_DIR" || { echo -e "${RED}No se encontró la ruta del proyecto.${NC}"; read -p "Enter..."; continue; }
      source "$PROJECT_DIR/ENTORNO/env/bin/activate"

      # Iniciamos y guardamos todo (errores y salida) en el log
      (python manage.py runserver ${BIND_ADDR}:8000 > "$LOG_FILE" 2>&1 &)
      
      # Esperamos un poco y verificamos si sigue vivo
      sleep 5

      if lsof -nP -iTCP:8000 -sTCP:LISTEN > /dev/null; then
          echo -e "${GREEN}Servidor iniciado correctamente 🚀${NC}"
          
          # Copiar URL
          SHARE_URL="http://${IP}:8000"
          echo -n "$SHARE_URL" | pbcopy
          echo -e "URL copiada: ${SHARE_URL}"
          
          echo -e "${BLUE}El servidor corre en segundo plano. Los errores se guardan en server_output.log${NC}"
          read -p "Presiona [Enter] para volver al menú..." dummy
      else
          # AQUÍ ESTÁ LA MAGIA: Si falla, te muestra por qué
          echo -e "${RED}❌ El servidor falló al iniciar. Mostrando últimos errores:${NC}"
          echo "-----------------------------------------------------"
          tail -n 20 "$LOG_FILE"
          echo "-----------------------------------------------------"
          echo -e "${YELLOW}Revisa el código anterior para ver el error de Python.${NC}"
          read -p "Presiona [Enter] para volver al menú..." dummy
      fi
      ;;
    2)
      PID=$(lsof -ti :8000)
      if [ -n "$PID" ]; then
        kill -9 $PID
        echo -e "${RED}Servidor detenido 🛑${NC}"
      else
        echo -e "${BLUE}No estaba corriendo.${NC}"
      fi
      sleep 1
      ;;
    3)
      # OPCIÓN NUEVA: Abrir el archivo de log directamente
      if [ -f "$LOG_FILE" ]; then
          echo -e "${GREEN}Abriendo registro de errores...${NC}"
          open "$LOG_FILE"
      else
          echo -e "${RED}No existe el archivo de log aún.${NC}"
          read -p "Enter..." dummy
      fi
      ;;
    4)
      echo -e "${BLUE}Saliendo... (El log se conserva en $LOG_FILE) 👋${NC}"
      exit 0
      ;;
    *)
      echo -e "${RED}Opción no válida.${NC}"
      sleep 1
      ;;
  esac
done