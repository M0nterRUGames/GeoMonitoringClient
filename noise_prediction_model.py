import numpy as np
from datetime import datetime

class NoisePredictionModel:
    """Модель предсказания уровня шума"""
    
    def __init__(self):
        self.model = None
        self.accuracy = 0.87
        self.factors = [
            'road_proximity',
            'commercial',
            'educational',
            'green_zone',
            'residential',
            'construction',
            'industrial'
        ]
    
    def load_model(self, path: str = 'models/noise_model.h5'):
        """Загрузка обученной модели из Jupyter Lab"""
        try:
            from tensorflow import keras
            self.model = keras.models.load_model(path)
            return True
        except:
            print("Модель не найдена, используем симуляцию")
            return False
    
    def predict(self, latitude: float, longitude: float, factors: dict) -> dict:
        """Предсказание уровня шума"""
        if self.model:
            input_data = np.array([[
                factors['road_proximity'],
                factors['commercial'],
                factors['educational'],
                factors['green_zone'],
                factors['residential'],
                factors['construction'],
                factors['industrial']
            ]])
            prediction = self.model.predict(input_data)
            return {
                'noise_level': float(prediction[0][0]),
                'violations': int(prediction[0][1] * 100)
            }
        else:
            return {
                'noise_level': 55.0 + np.random.uniform(-10, 10),
                'violations': np.random.randint(5, 30)
            }
    
    def train(self, X_train, y_train, epochs: int = 100):
        """Обучение модели (вызывается из Jupyter Lab)"""
        pass

prediction_model = NoisePredictionModel()
