from fbprophet import Prophet
from datetime import datetime as dt
from datetime import timedelta
from datetime import time
import pytz


def forecast_total(canal_df):
    utc = pytz.utc
    tz = pytz.timezone('America/Bogota')
    canal_df.columns = ['ds', 'y']
    m = Prophet(changepoint_prior_scale=2.5)
    m = m.add_seasonality(name='yearly', period=365, fourier_order=10)
    m = m.add_seasonality(name='monthly', period=30.5, fourier_order=5)
    m = m.fit(canal_df)
    last_date = tz.localize(dt.strptime(canal_df['ds'].iloc[-1],
                                        '%Y-%m-%d %H:%M:%S'))
    next_day = last_date + timedelta(1)
    midnight = tz.localize(dt.combine(next_day, time()))
    midnight_before = midnight - timedelta(1)
    tmp = canal_df['ds'].map(
        lambda x: tz.localize(dt.strptime(x, '%Y-%m-%d %H:%M:%S')))
    last_day = tmp > midnight_before
    predicted_values = canal_df[last_day]['y']
    predicted_dates = canal_df[last_day]['ds']
    total_now = sum(predicted_values)
    periods_left = int((midnight - last_date).seconds / 600) - 1
    print("periods left: " + str(periods_left))
    future = m.make_future_dataframe(periods=periods_left, freq='10min',
                                     include_history=False)
    print("future dates: ")
    print(future)
    fcst = m.predict(future)
    forecasts = list(fcst['yhat'])
    forecasts = dict([(str(date), yhat) if yhat >= 0 else (str(date), 0)
                      for date, yhat in
                      zip(list(future['ds']), forecasts)])
    total_then = int(sum(list(forecasts.values())) + total_now)
    total_lower = int(sum(fcst.tail(periods_left)['yhat_lower']) + total_now)
    total_upper = int(sum(fcst.tail(periods_left)['yhat_upper']) + total_now)
    return {'forcasted_total': total_then, 'current_total': int(total_now),
            'lower_limit': total_lower,
            'upper_limit': total_upper,
            'inidividual_predictions': forecasts}
