# Customer360 Admin Control Panel

Flask-based web admin dashboard for monitoring and controlling the Customer360 Data Platform.

## Features

### 📊 Real-Time Metrics Dashboard
- Total Revenue, Orders, Active Customers
- High-Risk Churn Count
- Kafka Topic Metrics (messages, lag)
- Warehouse Table Row Counts

### 🎛️ Pipeline Controls
- Start/Stop Kafka Producer
- Monitor Producer Status

### 🔍 Customer 360° Search
- Search by Customer ID
- View complete customer profile
- Show churn risk score
- Display purchase history

### 🚀 Airflow DAG Management
- List all DAGs with schedules
- Trigger DAGs manually
- View DAG status

### 🤖 ML Model Management
- View model metrics (AUC-ROC, precision)
- Trigger model retraining
- View last training date

### ✅ Data Quality Monitor
- Latest DQ check results
- Failed records count
- Alert on threshold breach

## Installation

```bash
# Install dependencies
pip install flask psycopg2-binary

# Or use the main requirements.txt
cd ..
pip install -r requirements.txt
```

## Running the Admin Panel

```bash
cd admin_panel
python app.py
```

The admin panel will be available at: **http://localhost:5000**

## Environment Variables

The admin panel uses the same environment variables as the main project:

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=customer360
POSTGRES_PASSWORD=customer360secret
POSTGRES_DB=customer360_warehouse
```

## API Endpoints

### Dashboard Stats
- `GET /api/stats` - Get quick stats for dashboard cards

### Kafka Metrics
- `GET /api/kafka/metrics` - Get Kafka topic metrics

### Warehouse
- `GET /api/warehouse/tables` - Get table row counts

### Customer Search
- `GET /api/customer/<customer_id>` - Get customer 360° view

### Data Quality
- `GET /api/data-quality` - Get latest DQ check results

### ML Model
- `GET /api/ml/model-info` - Get model metrics
- `POST /api/ml/train` - Trigger model training
- `POST /api/ml/score/<customer_id>` - Score individual customer

### Airflow
- `GET /api/airflow/dags` - List all DAGs
- `POST /api/airflow/trigger/<dag_id>` - Trigger a DAG

### Pipeline Control
- `GET /api/producer/status` - Get producer status
- `POST /api/producer/control` - Start/stop producer

## Screenshots

### Dashboard View
- 4 stat cards with key metrics
- Kafka metrics table
- Warehouse tables overview
- Customer search interface
- Airflow DAG list with trigger buttons
- ML model management

## Technologies Used

- **Backend**: Flask (Python)
- **Frontend**: Bootstrap 5
- **Icons**: Font Awesome 6
- **Database**: PostgreSQL (psycopg2)
- **Auto-refresh**: JavaScript (30-second interval)

## Production Deployment

For production use:

1. Use a production WSGI server (Gunicorn/uWSGI)
2. Enable authentication/authorization
3. Use HTTPS
4. Connect to Airflow REST API
5. Implement proper error handling
6. Add logging and monitoring

### Example with Gunicorn

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## Integration with Main Platform

The admin panel integrates with:
- PostgreSQL Warehouse (for stats and customer data)
- Airflow (for DAG management)
- ML Models (for model info and scoring)
- Kafka Producer (for pipeline control)

## Future Enhancements

- [ ] Real-time Kafka metrics from Prometheus
- [ ] WebSocket for live updates
- [ ] Advanced filtering and sorting
- [ ] Export data to CSV/Excel
- [ ] User authentication (OAuth2/LDAP)
- [ ] Alerting and notifications
- [ ] Historical trend charts
- [ ] Drag-and-drop dashboard customization
