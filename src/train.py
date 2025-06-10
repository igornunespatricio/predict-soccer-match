# %%
import pandas as pd
import numpy as np
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, confusion_matrix
import joblib

# %%
# load data
DATA_PATH = "../data/matches_feature_engineered.parquet"
df = pd.read_parquet(DATA_PATH)
print(df.info())

# %%
# Drop non-predictive columns
cols_to_drop = [
    "round",
    "match_date",
    "date_added",
    "home_team",
    "guest_team",
    "stadium",
    "score_home_team",
    "score_guest_team",
]
df.drop(columns=cols_to_drop, inplace=True)
df.info()

# %%
# drop nan rows
df = df.dropna()
df.info()

# %%
# 3. Encode target variable
target_col = "winning_team"
mapping = {"home": 0, "guest": 1, "draw": 2}
target_names = ["home", "guest", "draw"]
df[target_col] = df[target_col].map(mapping)
df.info()
# %%
# Separate features and labels
X = df.drop(columns=[target_col])
y = df[target_col]

# Convert any remaining object columns to numeric if needed
X = pd.get_dummies(X, drop_first=True)
X.info()
# %%
# Train/Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, stratify=y, random_state=42
)

# %%
# Train the model
model = RandomForestClassifier(n_estimators=200, random_state=42)
model.fit(X_train, y_train)

# %%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Get feature importances
importances = model.feature_importances_

# Match with column names
feature_names = X_train.columns
importance_df = pd.DataFrame({"feature": feature_names, "importance": importances})

# Sort by importance
importance_df = importance_df.sort_values(by="importance", ascending=False)

# Display top 20 features
print(importance_df)

# Optional: Plot
plt.figure(figsize=(12, 6))
sns.barplot(data=importance_df.head(20), x="importance", y="feature")
plt.title("Top 20 Feature Importances")
plt.tight_layout()
plt.show()


# %%
# Evaluate the model
y_pred = model.predict(X_test)
print(
    "Classification Report:\n",
    classification_report(y_test, y_pred, target_names=target_names),
)
print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))

# %%
# 8. Save the model and encoder
os.makedirs("models", exist_ok=True)
joblib.dump(model, "models/match_winner_model.pkl")
joblib.dump(label_encoder, "models/label_encoder.pkl")

print("✅ Model training complete. Files saved in /models")
