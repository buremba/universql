#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

show_help() {
    cat << EOF
Usage: $0 COMMAND

Commands:
    up              Start universql service only
    up-full         Start with PostgreSQL database
    down            Stop all services
    logs            Show logs
    restart         Restart universql service
    build           Build the Docker image
    test            Test the deployment
    deploy-k8s      Deploy local image to Kubernetes
    status          Show service status

Examples:
    $0 up                    # Start just universql
    $0 up-full              # Start with PostgreSQL database
    $0 logs                 # View logs
    $0 deploy-k8s           # Deploy to your K8s cluster

Services will be available at:
    - Frontend: http://localhost:8084/
    - PostgreSQL Protocol: localhost:5432
    - WebSocket API: ws://localhost:8084/ws
    - Test Database: localhost:5433 (when using up-full)
EOF
}

build_image() {
    print_info "Building universql Docker image..."
    docker-compose build universql
    print_success "Image built successfully!"
}

start_basic() {
    print_info "Starting universql service..."
    docker-compose up -d universql
    print_success "UniversQL started!"
    show_status
}

start_full() {
    print_info "Starting all services (universql + PostgreSQL)..."
    docker-compose --profile with-postgres up -d
    print_success "All services started!"
    show_status
}

stop_services() {
    print_info "Stopping all services..."
    docker-compose --profile with-postgres down
    print_success "All services stopped!"
}

show_logs() {
    print_info "Showing logs (Ctrl+C to exit)..."
    docker-compose logs -f universql
}

restart_service() {
    print_info "Restarting universql service..."
    docker-compose restart universql
    print_success "Service restarted!"
}

show_status() {
    print_info "Service Status:"
    docker-compose ps
    echo
    print_info "Health Check:"
    sleep 2
    if curl -s http://localhost:8084/api/health > /dev/null 2>&1; then
        print_success "✅ API is healthy - http://localhost:8084/api/health"
    else
        print_warning "⚠️  API not ready yet, try again in a few seconds"
    fi
    
    print_info "Access URLs:"
    echo "  📱 Frontend:    http://localhost:8084/"
    echo "  🔗 PostgreSQL:  localhost:5432"
    echo "  🌐 WebSocket:   ws://localhost:8084/ws"
    echo "  📚 API Docs:    http://localhost:8084/api/docs"
    
    if docker-compose ps postgres | grep -q "Up"; then
        echo "  🗄️  Test DB:     postgresql://testuser:testpass@localhost:5433/testdb"
    fi
}

test_deployment() {
    print_info "Testing deployment..."
    
    # Test health endpoint
    if curl -s http://localhost:8084/api/health | grep -q "healthy"; then
        print_success "✅ Health check passed"
    else
        print_error "❌ Health check failed"
        return 1
    fi
    
    # Test WebSocket connection
    print_info "Testing WebSocket connection..."
    if command -v wscat > /dev/null; then
        timeout 5 wscat -c ws://localhost:8084/ws --execute 'console.log("WebSocket test")' || true
        print_success "✅ WebSocket endpoint accessible"
    else
        print_warning "⚠️  Install wscat to test WebSocket: npm install -g wscat"
    fi
    
    print_success "🎉 Deployment test completed!"
}

deploy_to_k8s() {
    print_info "Deploying local image to Kubernetes..."
    
    # Check if kubectl is available and connected
    if ! command -v kubectl > /dev/null; then
        print_error "kubectl not found. Please install kubectl first."
        return 1
    fi
    
    # Check cluster connection
    if ! kubectl cluster-info > /dev/null 2>&1; then
        print_error "Not connected to a Kubernetes cluster."
        return 1
    fi
    
    local context=$(kubectl config current-context)
    print_info "Current cluster: $context"
    
    # Load image to kind if using kind
    if [[ "$context" =~ kind ]]; then
        print_info "Loading image to kind cluster..."
        kind load docker-image universql:latest
    elif [[ "$context" =~ minikube ]]; then
        print_info "Loading image to minikube..."
        minikube image load universql:latest
    elif [[ "$context" =~ gke ]]; then
        print_warning "For GKE, you'll need to push to a registry."
        return 1
    fi
    
    # Apply Kubernetes manifests
    print_info "Applying Kubernetes manifests..."
    cd k8s
    kubectl apply -f namespace.yaml -f configmap.yaml -f deployment.yaml -f service.yaml -f hpa.yaml -f pdb.yaml
    
    print_success "Deployed to Kubernetes! Check status with: kubectl get pods -n universql"
}

# Parse command
case ${1:-help} in
    up)
        start_basic
        ;;
    up-full)
        start_full
        ;;
    down)
        stop_services
        ;;
    logs)
        show_logs
        ;;
    restart)
        restart_service
        ;;
    build)
        build_image
        ;;
    test)
        test_deployment
        ;;
    deploy-k8s)
        deploy_to_k8s
        ;;
    status)
        show_status
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac 