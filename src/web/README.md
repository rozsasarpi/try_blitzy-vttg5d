# Electricity Market Price Forecasting System - Web Visualization

Dash-based visualization interface for the Electricity Market Price Forecasting System. This component provides an interactive dashboard for viewing probabilistic price forecasts for electricity market products.

## Features

The visualization interface provides the following features:

### Time Series Visualization
Interactive time series plots showing price forecasts over the 72-hour horizon with uncertainty bands

### Probabilistic Distribution View
Visualization of the probability distribution for selected forecast hours

### Hourly Values Table
Tabular display of forecast values with point forecasts and percentiles

### Product Comparison
Side-by-side comparison of different electricity market products

### Data Export
Export functionality for forecast data in CSV and Excel formats

### Responsive Design
Adaptive layout that works on desktop, tablet, and mobile devices

### Fallback Indicators
Clear indication when fallback forecasts are being displayed

## Architecture

The visualization component is built using the following technologies:

### Core Technologies
- Dash 2.9.0+: Interactive web application framework
- Plotly 5.14.0+: Data visualization library
- Dash Bootstrap Components 1.0.0+: Responsive layout components
- Pandas 2.0.0+: Data manipulation for forecast dataframes
- Flask 2.2.3+: Web server foundation for Dash

### Directory Structure
```
src/web/
├── app.py                # Main application entry point
├── wsgi.py               # WSGI entry point for production
├── config/               # Configuration settings
├── components/           # Dashboard UI components
├── layouts/              # Page layouts and structure
├── callbacks/            # Interactive callback functions
├── data/                 # Data loading and processing
├── utils/                # Utility functions
├── middleware/           # Request processing middleware
├── services/             # External service integrations
├── assets/               # Static assets (CSS, images, JS)
└── tests/                # Test suite
```

### Component Organization
The dashboard is organized into modular components:
- Control Panel: Product selection and visualization options
- Time Series: Main forecast visualization over time
- Probability Distribution: Distribution view for selected hours
- Forecast Table: Tabular data presentation
- Product Comparison: Multi-product visualization
- Export Panel: Data export functionality

## Setup and Installation

Instructions for setting up the visualization component:

### Prerequisites
- Python 3.10+
- pip or Poetry for dependency management
- Access to the backend forecasting API

### Local Development Setup
1. Clone the repository
2. Navigate to the `src/web` directory
3. Install dependencies: `pip install -r requirements.txt`
4. Create a `.env` file based on `.env.example`
5. Run the application: `python app.py`

### Docker Setup
1. Navigate to the `src/web` directory
2. Build the Docker image: `docker build -t forecast-visualization .`
3. Run the container: `docker run -p 8050:8050 -e API_BASE_URL=http://backend:8000/api forecast-visualization`

### Environment Variables
Key configuration options set via environment variables:

- `DEBUG`: Enable debug mode (default: False)
- `ENVIRONMENT`: Application environment (development, production)
- `API_BASE_URL`: URL for the backend API (default: http://localhost:8000/api)
- `SERVER_HOST`: Host to bind the server (default: 0.0.0.0)
- `SERVER_PORT`: Port to run the server (default: 8050)
- `DASHBOARD_REFRESH_INTERVAL_SECONDS`: Auto-refresh interval (default: 300)
- `ENABLE_RESPONSIVE_UI`: Enable responsive layout (default: True)
- `CACHE_ENABLED`: Enable data caching (default: True)
- `CACHE_TIMEOUT`: Cache timeout in seconds (default: 300)

## Usage

How to use the visualization interface:

### Accessing the Dashboard
The dashboard is available at `http://localhost:8050` when running locally, or at the configured host and port in production.

### Viewing Forecasts
1. Select a price product from the dropdown menu
2. Choose a date range for visualization
3. Toggle visualization options as needed
4. Interact with the time series plot to view specific hours
5. Export data using the export panel if needed

### Responsive Behavior
The dashboard automatically adapts to different screen sizes:
- Desktop: Full layout with side-by-side components
- Tablet: Stacked layout with full-width components
- Mobile: Simplified layout with collapsible sections

## Development

Guidelines for developing and extending the visualization component:

### Adding New Components
1. Create a new component file in the `components/` directory
2. Implement the component using Dash and Plotly
3. Add the component to the main dashboard layout in `layouts/main_dashboard.py`
4. Implement any required callbacks in the `callbacks/` directory

### Testing
Run tests using pytest:
```
pytest src/web/tests/
```

Test coverage includes:
- Component rendering tests
- Callback functionality tests
- Data processing tests
- Integration tests

### Code Style
Follow these guidelines for code consistency:
- Use Black for code formatting
- Follow PEP 8 style guidelines
- Add type hints to function signatures
- Document functions and components with docstrings

## API Integration

The visualization component integrates with the backend forecasting API to retrieve forecast data. Key integration points:

### Forecast Data Retrieval
- Endpoint: `{API_BASE_URL}/forecasts/latest`
- Method: GET
- Parameters: product, format
- Response: Forecast dataframe in JSON format

### Historical Forecast Retrieval
- Endpoint: `{API_BASE_URL}/forecasts/historical`
- Method: GET
- Parameters: date, product, format
- Response: Historical forecast dataframe in JSON format

### Forecast Status Check
- Endpoint: `{API_BASE_URL}/forecasts/status`
- Method: GET
- Response: JSON status object indicating forecast availability and generation time

## Deployment

Guidelines for deploying the visualization component to production:

### Production Deployment
For production deployment, use Gunicorn as the WSGI server:
```
gunicorn --bind 0.0.0.0:8050 --workers 2 --timeout 120 wsgi:server
```

### Docker Deployment
The provided Dockerfile creates a production-ready container:
1. Build the image: `docker build -t forecast-visualization .`
2. Run with appropriate environment variables:
```
docker run -p 8050:8050 \
  -e API_BASE_URL=http://backend:8000/api \
  -e ENVIRONMENT=production \
  -v forecast-cache:/app/cache \
  forecast-visualization
```

### Monitoring
Monitor the application using:
- Application logs (stdout/stderr from the container)
- Health check endpoint: `/health`
- Server metrics from Gunicorn

## Troubleshooting

Common issues and their solutions:

### Connection Issues
If the dashboard cannot connect to the backend API:
1. Verify the API_BASE_URL environment variable
2. Check that the backend service is running
3. Verify network connectivity between services
4. Check for any firewall or proxy issues

### Display Issues
If visualizations are not rendering correctly:
1. Clear browser cache
2. Check browser console for JavaScript errors
3. Verify that all required assets are loading
4. Test in different browsers

### Performance Issues
If the dashboard is slow to load or respond:
1. Enable caching (CACHE_ENABLED=True)
2. Increase worker count in Gunicorn
3. Optimize data loading and processing
4. Consider adding a CDN for static assets

## License

This component is part of the Electricity Market Price Forecasting System and is subject to the project's licensing terms.