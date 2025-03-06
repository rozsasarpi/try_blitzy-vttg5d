### WHY - Vision & Purpose

#### Purpose & Users

What does your application do?: day-ahead market stage (before day-ahead market closure) price forecast for DALMP, RTLMP, and all ancillary service products  
  
Who will use it?:  it will be used by traders (humans) and downstream code, i.e. an optimization algorithm that manages a batters

Why will they use it instead of alternatives?: because the forecast performance (accuracy) is higher

### WHAT - Core Requirements

#### Functional Requirements

What action needs to happen? it must produce forecasts for all price products (DALMP, RTLMP, all ancillary service (AS) products; once a day at 7 am CST, hourly granularity forecasts over 72 hours, starting from the beginning of the next day after inference is made (the day after the corresponding market closure), the forecasts should be probabilistic

What should the outcome be? 

must: each day the forecasts are saved as a pandas dataframe, timestamped, sample-based forecasts are included (the dataframe follows a pandera schema)

must: visualize the price forecasts, time vs price

### HOW - Planning & Implementation

#### Technical Implementation

it should run on a schedule; there must be a simple fallback forecast: yesterday's forecast  
the model must be a simple linear model, one model for each unique forecast/target hour and price product combination, i.e. (DALMP, hour ending 5)  
load forecast, lagged historical prices, generation forecasts (per fuel source type) are available  
everything must be implemented in python  
please use a functional approach when implementing (only use classes if you must)  
visualization: dash