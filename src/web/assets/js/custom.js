/**
 * custom.js
 * Custom JavaScript functionality for the Electricity Market Price Forecasting System
 * 
 * This file enhances the Dash-based visualization interface with client-side
 * functionality to improve user experience, handle responsive design, and
 * optimize performance by reducing server roundtrips.
 * 
 * Version: 1.0.0
 */

// ============================================================================
// GLOBAL CONSTANTS
// ============================================================================

/**
 * Viewport breakpoints (in pixels) for responsive design
 */
const VIEWPORT_BREAKPOINTS = { mobile: 576, tablet: 992, desktop: Infinity };

/**
 * Delay in milliseconds for debouncing functions
 */
const DEBOUNCE_DELAY = 250;

/**
 * Delay in milliseconds for throttling chart interactions
 */
const CHART_INTERACTION_DELAY = 100;

/**
 * Component IDs used to access Dash elements
 */
const COMPONENT_IDS = {
    timeSeriesChart: 'time-series-chart',
    distributionChart: 'probability-distribution-chart',
    forecastTable: 'forecast-table',
    productDropdown: 'product-dropdown',
    dateRangePicker: 'date-range-picker',
    visualizationOptions: 'visualization-options',
    selectedTimestampStore: 'selected-timestamp-store',
    viewportStore: 'viewport-size-store'
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Detects the current viewport size category based on window width
 * 
 * @returns {string} Viewport size category ('mobile', 'tablet', or 'desktop')
 */
function detectViewportSize() {
    const width = window.innerWidth;
    
    if (width < VIEWPORT_BREAKPOINTS.mobile) {
        return 'mobile';
    } else if (width < VIEWPORT_BREAKPOINTS.tablet) {
        return 'tablet';
    } else {
        return 'desktop';
    }
}

/**
 * Updates the viewport store component with the current viewport size
 */
function updateViewportStore() {
    const viewportSize = detectViewportSize();
    const viewportStoreElement = document.getElementById(COMPONENT_IDS.viewportStore);
    
    if (viewportStoreElement) {
        viewportStoreElement.setAttribute('data-viewport', viewportSize);
        console.log(`Viewport updated to: ${viewportSize}`);
    }
}

/**
 * Creates a debounced version of a function that delays execution until after wait milliseconds
 * 
 * @param {Function} func - The function to debounce
 * @param {number} wait - The number of milliseconds to delay
 * @returns {Function} Debounced function
 */
function debounce(func, wait) {
    let timeout;
    
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * Creates a throttled version of a function that only invokes at most once per specified period
 * 
 * @param {Function} func - The function to throttle
 * @param {number} limit - The time limit in milliseconds
 * @returns {Function} Throttled function
 */
function throttle(func, limit) {
    let inThrottle;
    
    return function executedFunction(...args) {
        if (!inThrottle) {
            func(...args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// ============================================================================
// CHART INTERACTION FUNCTIONS
// ============================================================================

/**
 * Handles click events on time series charts to update the probability distribution view
 * 
 * @param {Event} event - The click event
 */
function handleTimeSeriesClick(event) {
    // Extract the clicked timestamp from event data
    // Note: The exact structure depends on Plotly's event data format
    if (event && event.points && event.points.length > 0) {
        const point = event.points[0];
        const timestamp = point.x;
        
        // Update the selected timestamp store
        const timestampStoreElement = document.getElementById(COMPONENT_IDS.selectedTimestampStore);
        if (timestampStoreElement) {
            timestampStoreElement.setAttribute('data-timestamp', timestamp);
            console.log(`Selected timestamp: ${timestamp}`);
            
            // This will trigger any Dash callbacks that depend on this component
            const changeEvent = new Event('change');
            timestampStoreElement.dispatchEvent(changeEvent);
        }
    }
}

/**
 * Handles hover events on time series charts to show tooltips and highlight related data
 * 
 * @param {Event} event - The hover event
 */
function handleTimeSeriesHover(event) {
    if (event && event.points && event.points.length > 0) {
        const point = event.points[0];
        
        // Create and position tooltip
        const tooltipContent = formatTooltipContent(point);
        
        // Depending on the chart library, you might need to handle this differently
        // This is a simplified example assuming custom tooltip implementation
        const tooltip = document.getElementById('custom-tooltip');
        if (tooltip) {
            tooltip.innerHTML = tooltipContent;
            tooltip.style.display = 'block';
            tooltip.style.left = `${event.event.clientX + 10}px`;
            tooltip.style.top = `${event.event.clientY + 10}px`;
        }
    }
}

/**
 * Formats data for display in tooltips
 * 
 * @param {object} data - The data point to format
 * @returns {string} HTML content for tooltip
 */
function formatTooltipContent(data) {
    // Format timestamp
    const timestamp = new Date(data.x).toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit'
    });
    
    // Format price with 2 decimal places
    const price = typeof data.y === 'number' ? data.y.toFixed(2) : data.y;
    
    // Generate HTML content
    return `
        <div class="tooltip-content">
            <div class="tooltip-timestamp">${timestamp}</div>
            <div class="tooltip-price">$${price}</div>
            ${data.customdata ? `<div class="tooltip-additional">${data.customdata}</div>` : ''}
        </div>
    `;
}

// ============================================================================
// RESPONSIVE DESIGN FUNCTIONS
// ============================================================================

/**
 * Adjusts dashboard layout based on current viewport size
 */
function adjustLayoutForViewport() {
    const viewportSize = detectViewportSize();
    
    // Get main layout container
    const dashboardContainer = document.querySelector('.dashboard-container');
    if (!dashboardContainer) return;
    
    // Remove existing responsive classes
    dashboardContainer.classList.remove('layout-mobile', 'layout-tablet', 'layout-desktop');
    
    // Add appropriate class for current viewport
    dashboardContainer.classList.add(`layout-${viewportSize}`);
    
    // Adjust chart dimensions
    const timeSeriesChart = document.getElementById(COMPONENT_IDS.timeSeriesChart);
    const distributionChart = document.getElementById(COMPONENT_IDS.distributionChart);
    
    if (timeSeriesChart && window.Plotly) {
        // Apply different size settings based on viewport
        if (viewportSize === 'mobile') {
            // Adjust for mobile
            Plotly.relayout(timeSeriesChart, {
                height: 250,
                margin: { t: 30, r: 10, b: 40, l: 40 },
                'font.size': 10
            });
        } else if (viewportSize === 'tablet') {
            // Adjust for tablet
            Plotly.relayout(timeSeriesChart, {
                height: 350,
                margin: { t: 40, r: 20, b: 50, l: 50 },
                'font.size': 11
            });
        } else {
            // Adjust for desktop
            Plotly.relayout(timeSeriesChart, {
                height: 450,
                margin: { t: 50, r: 30, b: 60, l: 60 },
                'font.size': 12
            });
        }
    }
    
    if (distributionChart && window.Plotly) {
        // Similar adjustments for distribution chart
        if (viewportSize === 'mobile') {
            Plotly.relayout(distributionChart, {
                height: 200,
                margin: { t: 30, r: 10, b: 40, l: 40 },
                'font.size': 10
            });
        } else if (viewportSize === 'tablet') {
            Plotly.relayout(distributionChart, {
                height: 250,
                margin: { t: 40, r: 20, b: 50, l: 50 },
                'font.size': 11
            });
        } else {
            Plotly.relayout(distributionChart, {
                height: 300,
                margin: { t: 50, r: 30, b: 60, l: 60 },
                'font.size': 12
            });
        }
    }
    
    // Adjust table pagination
    const forecastTable = document.getElementById(COMPONENT_IDS.forecastTable);
    if (forecastTable) {
        const pageSize = viewportSize === 'mobile' ? 5 : viewportSize === 'tablet' ? 8 : 12;
        // This approach may vary depending on how the table is implemented
        // This example assumes a custom pagination system
        if (window.dashboardState) {
            window.dashboardState.tablePageSize = pageSize;
            // Trigger table refresh if needed
        }
    }
    
    console.log(`Layout adjusted for ${viewportSize} viewport`);
}

/**
 * Sets up event listeners for responsive behavior
 */
function setupResponsiveListeners() {
    // Debounce resize event to avoid excessive calculations
    const debouncedUpdateViewport = debounce(updateViewportStore, DEBOUNCE_DELAY);
    const debouncedAdjustLayout = debounce(adjustLayoutForViewport, DEBOUNCE_DELAY);
    
    // Add event listeners
    window.addEventListener('resize', debouncedUpdateViewport);
    window.addEventListener('resize', debouncedAdjustLayout);
    
    // Initial setup
    updateViewportStore();
    adjustLayoutForViewport();
    
    console.log('Responsive listeners initialized');
}

/**
 * Sets up event listeners for chart interactions
 */
function setupChartInteractions() {
    const timeSeriesChart = document.getElementById(COMPONENT_IDS.timeSeriesChart);
    
    if (timeSeriesChart && window.Plotly) {
        // For Plotly charts, we use the plotly_click event
        timeSeriesChart.on('plotly_click', handleTimeSeriesClick);
        
        // Throttle hover events to prevent performance issues
        const throttledHover = throttle(handleTimeSeriesHover, CHART_INTERACTION_DELAY);
        timeSeriesChart.on('plotly_hover', throttledHover);
        
        // Hide tooltip on mouseout
        timeSeriesChart.on('plotly_unhover', () => {
            const tooltip = document.getElementById('custom-tooltip');
            if (tooltip) {
                tooltip.style.display = 'none';
            }
        });
    }
    
    console.log('Chart interactions initialized');
}

// ============================================================================
// INITIALIZATION
// ============================================================================

/**
 * Initializes all custom JavaScript behaviors when the page loads
 */
function initializeCustomBehaviors() {
    // Setup responsive design handlers
    setupResponsiveListeners();
    
    // Setup chart interaction handlers
    setupChartInteractions();
    
    // Initial layout adjustment
    adjustLayoutForViewport();
    
    console.log('Custom behaviors initialized');
}

// Initialize when DOM is fully loaded
document.addEventListener('DOMContentLoaded', initializeCustomBehaviors);

// For Dash applications, we need additional initialization for dynamically updated components
// using MutationObserver to detect when Dash updates the DOM
const dashAppObserver = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
        if (mutation.type === 'childList' && mutation.addedNodes.length > 0) {
            // Check if any of our watched components were added/updated
            const chartUpdated = Array.from(mutation.addedNodes).some(node => 
                node.id === COMPONENT_IDS.timeSeriesChart || 
                node.id === COMPONENT_IDS.distributionChart);
                
            if (chartUpdated) {
                // Re-initialize chart interactions if charts were updated
                setupChartInteractions();
                adjustLayoutForViewport();
            }
        }
    }
});

// Start observing once the dashboard container is available
document.addEventListener('DOMContentLoaded', () => {
    const dashboardContainer = document.querySelector('.dash-container');
    if (dashboardContainer) {
        dashAppObserver.observe(dashboardContainer, { childList: true, subtree: true });
    }
});