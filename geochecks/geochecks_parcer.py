import requests
import pandas as pd
import json
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
import numpy as np
import os
import datetime
import time

current_time = datetime.datetime.now().strftime("%Y%m%d")

cookies = {
    '_ym_uid': '1725367295143723997',
    '_ym_d': '1725367295',
    '_ym_isad': '1',
    '_ym_visorc': 'w',
}

headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
    'Authorization': 'Bearer <VALID_TOKEN>',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
}

def download_geocheki(filename, shp):
    polygon = gpd.read_file(shp)
    gdf = gpd.GeoDataFrame(polygon, crs=3857)
    xmin, ymin, xmax, ymax = gdf.total_bounds

    side = 100000
    cols = np.arange(xmin, xmax + side, side)
    rows = np.arange(ymin, ymax + side, side)

    polygons = [
        Polygon([(x, y), (x + side, y), (x + side, y + side), (x, y + side)])
        for x in cols[:-1] for y in rows[:-1]
    ]

    grid = gpd.GeoDataFrame({'geometry': polygons}, crs=3857)
    os.makedirs(f"\\geography\\{filename}", exist_ok=True)
    grid.to_file(f"\\geography\\{filename}\\grid_{filename}.shp")

    for i, (_, small_poly) in enumerate(grid.iterrows()):
        try:
            xmin, ymin, xmax, ymax = small_poly.geometry.bounds
            params = {
                'NorthEast': [int(xmax), int(ymax)],
                'SouthWest': [int(xmin), int(ymin)],
                'H3Resolution': '10',
            }
            r = requests.get('https://geochecki-vpd.nalog.gov.ru/api/Metrics/bounds', params=params, cookies=cookies, headers=headers)
            r.raise_for_status()

            r_dict = r.json()
            df = pd.json_normalize(r_dict.get("features", []))

            data = {"geometry": []}
            for _, row in df.iterrows():
                coords = row.get('geometry.coordinates', [])
                if len(coords) == 1:
                    data["geometry"].append(Polygon(coords[0]))
                else:
                    data["geometry"].append(MultiPolygon([Polygon(c) for c in coords]))

            data_prepared = pd.DataFrame(data).join(df)
            gdf = gpd.GeoDataFrame(data_prepared, geometry='geometry').set_crs("EPSG:4326")

            # Обработка данных
            for col in ["KktCount", "AverageBill", "TruncatedAverageBill", "MedianBill", "CacheBillPercent", "CachePayPercent"]:
                gdf[f'{col}1'] = gdf[f'properties.{col}'].apply(lambda x: x[0] if isinstance(x, list) else None)
                gdf[f'{col}2'] = gdf[f'properties.{col}'].apply(lambda x: x[1] if isinstance(x, list) else None)

            # Сохранение
            gdf.to_file(f"\\geography\\{filename}\\output_geojson\\{filename}_{i}.json", driver="GeoJSON")
            gdf.to_excel(f"\\geography\\{filename}\\output_excel\\{filename}_check_{i}.xlsx")
            print(f"Полигон №{i} обработан успешно")
            time.sleep(1)  # Пауза для предотвращения блокировок
        except Exception as e:
            print(f"Ошибка обработки полигона №{i}: {e}")

def merge_json_files(directory_path):
    merged_features = []  # Список для хранения всех 'features'

    for filename in os.listdir(directory_path):
        if filename.endswith('.json'):  # Проверяем, что файл имеет расширение .json
            with open(os.path.join(directory_path, filename), 'r', encoding='utf-8') as file:
                data = json.load(file)  # Загружаем содержимое JSON
                if data.get("type") == "FeatureCollection":  # Проверяем, что файл — GeoJSON
                    merged_features.extend(data["features"])  # Добавляем все 'features' в общий список
                else:
                    print(f"Файл {filename} не является GeoJSON или имеет некорректную структуру.")

    # Создаем итоговый объект GeoJSON
    merged_geojson = {
        "type": "FeatureCollection",
        "features": merged_features
    }

    return merged_geojson


if __name__ == "__main__":
    shp_directory_path = "\\shp"
    for filename in os.listdir(shp_directory_path):
        if filename.endswith('.shp'):
            shp = os.path.join(shp_directory_path, filename)
            download_geocheki(filename.split('.')[0], shp)
            directory_path = f"E:\\Урбаника\\Илья\\2024.12\\geocheki\\geography\\{filename.split('.')[0]}"
            merged_data = merge_json_files(f"{directory_path}\\output_geojson")
            output_file = f"{directory_path}\\merge_json_{current_time}.json"
            with open(output_file, 'w', encoding='utf-8') as outfile:
                json.dump(merged_data, outfile, ensure_ascii=False)
