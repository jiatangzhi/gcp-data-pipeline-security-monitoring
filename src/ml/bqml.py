"""
ml/bqml.py
----------
BigQuery ML models for security anomaly detection.
All training and inference runs inside BigQuery — no data movement needed.

Models:
  login_anomaly_model   — K-Means clustering on login behaviour
                          labels users as NORMAL / SUSPICIOUS / HIGH_RISK
  risk_classifier       — Logistic regression to predict if a user is high risk
                          based on failed_attempts and total_logins

Usage:
  from src.ml.bqml import BQMLModels
  ml = BQMLModels()
  ml.train_login_anomaly_model()
  df = ml.predict_login_anomalies()
"""

import os
import logging
import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT", "my-gcp-project")
DATASET_ID = os.environ.get("BQ_DATASET", "security_monitoring")


def _ref(table: str) -> str:
    return f"`{PROJECT_ID}.{DATASET_ID}.{table}`"


class BQMLModels:
    """Manages BigQuery ML model training and inference for security analytics."""

    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)

    def _run(self, sql: str) -> pd.DataFrame:
        logger.info(f"Running BQML query: {sql[:80]}...")
        return self.client.query(sql).result().to_dataframe()

    # ─── Model 1: Login Anomaly Clustering (K-Means) ─────────────────────────

    def train_login_anomaly_model(self):
        """
        Train a K-Means clustering model on login behaviour.
        3 clusters map to: normal users, suspicious, high-risk.

        Trained on agg_failed_login_rate — one row per day with:
          total_logins, failed_logins, failed_login_rate
        """
        sql = f"""
        CREATE OR REPLACE MODEL {_ref('login_anomaly_model')}
        OPTIONS(
            model_type    = 'kmeans',
            num_clusters  = 3,
            standardize_features = TRUE
        ) AS
        SELECT
            total_logins,
            failed_logins,
            failed_login_rate
        FROM {_ref('agg_failed_login_rate')}
        """
        logger.info("Training login_anomaly_model (K-Means)...")
        self.client.query(sql).result()
        logger.info("login_anomaly_model trained")

    def predict_login_anomalies(self) -> pd.DataFrame:
        """
        Run the K-Means model on agg_failed_login_rate.
        Returns each day with its assigned cluster (CENTROID_ID).
        Higher centroid_id = more anomalous (inspect cluster centroids to confirm).
        """
        sql = f"""
        SELECT
            r.date,
            r.total_logins,
            r.failed_logins,
            r.failed_login_rate,
            p.CENTROID_ID AS cluster
        FROM
            ML.PREDICT(
                MODEL {_ref('login_anomaly_model')},
                TABLE {_ref('agg_failed_login_rate')}
            ) p
        JOIN {_ref('agg_failed_login_rate')} r
          ON r.total_logins   = p.total_logins
         AND r.failed_logins  = p.failed_logins
        ORDER BY r.date
        """
        return self._run(sql)

    # ─── Model 2: High-Risk User Classifier (Logistic Regression) ────────────

    def train_risk_classifier(self):
        """
        Train a logistic regression classifier to predict HIGH risk users.
        Label: risk_level = 'HIGH' (from agg_suspicious_users).
        Features: failed_attempts.
        """
        sql = f"""
        CREATE OR REPLACE MODEL {_ref('risk_classifier')}
        OPTIONS(
            model_type    = 'logistic_reg',
            input_label_cols = ['is_high_risk']
        ) AS
        SELECT
            failed_attempts,
            IF(risk_level = 'HIGH', 1, 0) AS is_high_risk
        FROM {_ref('agg_suspicious_users')}
        """
        logger.info("Training risk_classifier (Logistic Regression)...")
        self.client.query(sql).result()
        logger.info("risk_classifier trained")

    def predict_user_risk(self) -> pd.DataFrame:
        """
        Score all suspicious users with the risk classifier.
        Returns predicted label + probability for each user.
        """
        sql = f"""
        SELECT
            s.user_id,
            s.failed_attempts,
            s.risk_level                     AS rule_based_risk,
            p.predicted_is_high_risk         AS ml_predicted_high_risk,
            ROUND(p.predicted_is_high_risk_probs[OFFSET(1)].prob, 4) AS high_risk_probability
        FROM
            ML.PREDICT(
                MODEL {_ref('risk_classifier')},
                TABLE {_ref('agg_suspicious_users')}
            ) p
        JOIN {_ref('agg_suspicious_users')} s USING (failed_attempts)
        ORDER BY high_risk_probability DESC
        """
        return self._run(sql)

    # ─── Model evaluation ────────────────────────────────────────────────────

    def evaluate_risk_classifier(self) -> pd.DataFrame:
        """Return precision, recall, F1 and other metrics for the classifier."""
        sql = f"""
        SELECT *
        FROM ML.EVALUATE(MODEL {_ref('risk_classifier')})
        """
        return self._run(sql)

    def get_cluster_centroids(self) -> pd.DataFrame:
        """Return the centroid feature values for the K-Means model."""
        sql = f"""
        SELECT *
        FROM ML.CENTROIDS(MODEL {_ref('login_anomaly_model')})
        ORDER BY centroid_id
        """
        return self._run(sql)
