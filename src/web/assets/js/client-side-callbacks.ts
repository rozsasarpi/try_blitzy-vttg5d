/**
 * Client-side callbacks for the Electricity Market Price Forecasting System's Dash visualization
 * 
 * This module contains functions that run in the browser to handle responsive design
 * adjustments, performance-critical UI interactions, and viewport detection without
 * requiring server roundtrips.
 * 
 * @version 1.0.0
 */

// Define viewport breakpoints for responsive design
export const VIEWPORT_BREAKPOINTS = {
  mobile: 576,  // Max width for mobile devices
  tablet: 992   // Max width for tablet devices
};

// Store ID for viewport information in Dash client-side storage
export const VIEWPORT_STORE_ID = 'viewport-store';

// Type definitions for better TypeScript support
type ViewportSize = 'mobile' | 'tablet' | 'desktop';

interface ViewportData {
  width: number;
  height: number;
  size: ViewportSize;
}

interface Margins {
  l: number;
  r: number;
  t: number;
  b: number;
  pad: number;
}

// Add type declaration for Dash client-side functionality
declare global {
  interface Window {
    dash_clientside: {
      set_props: (props: any, storeId: string) => void;
    };
  }
}

/**
 * Detects the current viewport size category based on window width
 * @param width - The current window width in pixels
 * @returns Viewport size category (mobile, tablet, or desktop)
 */
export function detectViewportSize(width: number): ViewportSize {
  if (width <= VIEWPORT_BREAKPOINTS.mobile) {
    return 'mobile';
  } else if (width <= VIEWPORT_BREAKPOINTS.tablet) {
    return 'tablet';
  } else {
    return 'desktop';
  }
}

/**
 * Updates the viewport store with current window dimensions
 * @param width - Current window width in pixels
 * @param height - Current window height in pixels
 * @returns Object containing width, height, and viewport size category
 */
export function updateViewportStore(width: number, height: number): ViewportData {
  const size = detectViewportSize(width);
  return {
    width,
    height,
    size
  };
}

/**
 * Dynamically adjusts graph height based on viewport size
 * @param viewportData - Current viewport data
 * @param baseHeight - Base height to adjust from
 * @returns Updated layout object with adjusted height
 */
export function adjustGraphHeight(viewportData: ViewportData, baseHeight: number): { height: number } {
  const { size } = viewportData;
  
  // Height adjustment factors for different viewport sizes
  const heightFactors = {
    mobile: 0.7,   // 70% of base height on mobile
    tablet: 0.85,  // 85% of base height on tablet
    desktop: 1.0   // 100% of base height on desktop
  };
  
  const adjustedHeight = baseHeight * heightFactors[size];
  
  return {
    height: adjustedHeight
  };
}

/**
 * Toggles the visibility of the control panel on mobile devices
 * @param n_clicks - Number of button clicks (from Dash)
 * @param currentStyle - Current style object of the control panel
 * @param viewportData - Current viewport data
 * @returns Updated style object with visibility properties
 */
export function toggleControlPanel(
  n_clicks: number,
  currentStyle: Record<string, any>,
  viewportData: ViewportData
): Record<string, any> {
  // Only toggle on mobile devices
  if (viewportData.size !== 'mobile') {
    return currentStyle;
  }
  
  // Default style if currentStyle is null or undefined
  const style = currentStyle || {};
  
  // Toggle display property
  const isVisible = style.display !== 'none';
  
  return {
    ...style,
    display: isVisible ? 'none' : 'block'
  };
}

/**
 * Adjusts font size based on viewport size
 * @param viewportData - Current viewport data
 * @param baseSize - Base font size in pixels
 * @returns Style object with adjusted font size
 */
export function adjustFontSize(viewportData: ViewportData, baseSize: number): { fontSize: string } {
  const { size } = viewportData;
  
  // Font size adjustment factors for different viewport sizes
  const fontSizeFactors = {
    mobile: 0.8,   // 80% of base size on mobile
    tablet: 0.9,   // 90% of base size on tablet
    desktop: 1.0   // 100% of base size on desktop
  };
  
  const adjustedSize = baseSize * fontSizeFactors[size];
  
  return {
    fontSize: `${adjustedSize}px`
  };
}

/**
 * Optimizes graph margins based on viewport size
 * @param viewportData - Current viewport data
 * @param baseMargins - Base margins object
 * @returns Layout object with optimized margins
 */
export function optimizeGraphMargins(
  viewportData: ViewportData,
  baseMargins: Margins
): { margin: Margins } {
  const { size } = viewportData;
  
  // Margin adjustment factors for different viewport sizes
  const marginFactors = {
    mobile: 0.5,    // 50% of base margins on mobile
    tablet: 0.75,   // 75% of base margins on tablet
    desktop: 1.0    // 100% of base margins on desktop
  };
  
  const factor = marginFactors[size];
  
  // Apply the factor to each margin
  const optimizedMargins: Margins = {
    l: Math.round(baseMargins.l * factor),
    r: Math.round(baseMargins.r * factor),
    t: Math.round(baseMargins.t * factor),
    b: Math.round(baseMargins.b * factor),
    pad: Math.round(baseMargins.pad * factor)
  };
  
  return {
    margin: optimizedMargins
  };
}

/**
 * Adjusts table pagination settings based on viewport size
 * @param viewportData - Current viewport data
 * @returns Pagination configuration object
 */
export function handleTablePagination(viewportData: ViewportData): { page_size: number } {
  const { size } = viewportData;
  
  // Page sizes for different viewport sizes
  const pageSizes = {
    mobile: 5,     // Show 5 rows on mobile
    tablet: 10,    // Show 10 rows on tablet
    desktop: 15    // Show 15 rows on desktop
  };
  
  return {
    page_size: pageSizes[size]
  };
}

/**
 * Sets up event listeners for viewport size detection
 * Initializes and keeps viewport store updated
 */
export function setupViewportDetection(): void {
  // Don't execute during server-side rendering
  if (typeof window === 'undefined') return;
  
  // Function to update store with current dimensions
  const updateDimensions = (): void => {
    const width = window.innerWidth;
    const height = window.innerHeight;
    
    // Use window.dash_clientside.set_props to update the store
    // This assumes Dash's client-side callback is available
    if (window.dash_clientside) {
      window.dash_clientside.set_props(
        updateViewportStore(width, height),
        VIEWPORT_STORE_ID
      );
    }
  };
  
  // Throttle function to limit execution frequency during resize
  let resizeTimeout: number | null = null;
  const throttledUpdateDimensions = (): void => {
    if (resizeTimeout === null) {
      resizeTimeout = window.setTimeout(() => {
        resizeTimeout = null;
        updateDimensions();
      }, 200); // Update at most every 200ms during resize
    }
  };
  
  // Set up the resize event listener
  window.addEventListener('resize', throttledUpdateDimensions);
  
  // Initial update
  updateDimensions();
}