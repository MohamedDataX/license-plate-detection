#!/bin/bash
# ==============================================
# License Plate Detection Startup Script
# ==============================================

#colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' 

echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║       License Plate Detection - Startup Script           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Check current directory
if [ ! -f "api.py" ] || [ ! -f "app.py" ]; then
    echo -e "${RED}Error: Please run this script from the 3-web-interface directory${NC}"
    exit 1
fi

# Stop processes
cleanup() {
    echo -e "\n${YELLOW}Stopping services...${NC}"
    if [ ! -z "$API_PID" ]; then
        kill $API_PID 2>/dev/null
    fi
    if [ ! -z "$STREAMLIT_PID" ]; then
        kill $STREAMLIT_PID 2>/dev/null
    fi
    exit 0
}

trap cleanup SIGINT SIGTERM

#selection mode
MODE=${1:-"both"}

case $MODE in
    "api")
        echo -e "${GREEN}Starting FastAPI only...${NC}"
        echo -e "${BLUE}   URL: http://localhost:8000${NC}"
        echo -e "${BLUE}   Docs: http://localhost:8000/docs${NC}"
        python3 api.py
        ;;
    "streamlit")
        echo -e "${GREEN}Starting Streamlit only...${NC}"
        echo -e "${BLUE}   URL: http://localhost:8501${NC}"
        streamlit run app.py --server.port 8501
        ;;
    "both")
        echo -e "${GREEN}Starting FastAPI + Streamlit...${NC}"
        echo ""
        echo -e "${BLUE}   FastAPI:   http://localhost:8000${NC}"
        echo -e "${BLUE}   API Docs:  http://localhost:8000/docs${NC}"
        echo -e "${BLUE}   Streamlit: http://localhost:8501${NC}"
        echo ""

        # Start FastAPI in background
        echo -e "${YELLOW}Starting FastAPI...${NC}"
        python3 api.py &
        API_PID=$!

        # Wait for API to be ready
        sleep 3

        # Start Streamlit
        echo -e "${YELLOW}Starting Streamlit...${NC}"
        streamlit run app.py --server.port 8501 &
        STREAMLIT_PID=$!

        echo ""
        echo -e "${GREEN}Both services started!${NC}"
        echo -e "${YELLOW}Press Ctrl+C to stop all services${NC}"

        # Wait for processes
        wait
        ;;
    *)
        echo -e "${RED}Usage: $0 [api|streamlit|both]${NC}"
        echo "  api       - Start FastAPI only"
        echo "  streamlit - Start Streamlit only"
        echo "  both      - Start both (default)"
        exit 1
        ;;
esac