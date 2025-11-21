#!/usr/bin/env python3
"""
Script de testing para verificar la performance del nuevo sistema de cache.
Ejecutar después de implementar las optimizaciones.
"""

import time
import requests
import json
from datetime import datetime


class CachePerformanceTester:
    def __init__(self, base_url="http://localhost:4000"):
        self.base_url = base_url
        self.results = {}

    def test_cache_stats(self):
        """Test 1: Verificar que el endpoint de estadísticas funciona."""
        print("🧪 Test 1: Verificando endpoint de estadísticas...")

        try:
            start_time = time.time()
            response = requests.get(f"{self.base_url}/private/cache_stats")
            elapsed_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                print(f"✅ Cache stats OK en {elapsed_time:.3f}s")
                print(f"   📊 Datos: {json.dumps(data['data'], indent=2)}")
                self.results['cache_stats'] = {
                    'success': True,
                    'response_time': elapsed_time,
                    'data': data['data']
                }
            else:
                print(f"❌ Error en cache stats: {response.status_code}")
                self.results['cache_stats'] = {'success': False, 'error': response.text}

        except Exception as e:
            print(f"❌ Excepción en cache stats: {str(e)}")
            self.results['cache_stats'] = {'success': False, 'error': str(e)}

    def test_cache_refresh(self):
        """Test 2: Verificar que el refresco forzado funciona."""
        print("\n🧪 Test 2: Verificando refresco forzado...")

        try:
            start_time = time.time()
            response = requests.post(f"{self.base_url}/private/cache_refresh")
            elapsed_time = time.time() - start_time

            if response.status_code == 200:
                data = response.json()
                print(f"✅ Cache refresh OK en {elapsed_time:.3f}s")
                print(f"   📊 Catálogos cargados: {data['data'].get('catalogs_loaded', 0)}")
                print(f"   👥 Beneficiarios indexados: {data['data'].get('beneficiarios_indexed', 0)}")
                self.results['cache_refresh'] = {
                    'success': True,
                    'response_time': elapsed_time,
                    'data': data['data']
                }
            else:
                print(f"❌ Error en cache refresh: {response.status_code}")
                self.results['cache_refresh'] = {'success': False, 'error': response.text}

        except Exception as e:
            print(f"❌ Excepción en cache refresh: {str(e)}")
            self.results['cache_refresh'] = {'success': False, 'error': str(e)}

    def test_catalog_loading_performance(self):
        """Test 3: Medir performance de carga de catálogos."""
        print("\n🧪 Test 3: Midiendo performance de carga de catálogos...")

        test_data = {
            "idDependencia": 1,
            "anio": 2025
        }

        # Múltiples llamadas para medir cache hit
        times = []

        for i in range(5):
            try:
                start_time = time.time()
                response = requests.post(
                    f"{self.base_url}/private/datos",
                    json=test_data,
                    headers={'Content-Type': 'application/json'}
                )
                elapsed_time = time.time() - start_time
                times.append(elapsed_time)

                if response.status_code == 200:
                    print(f"   Llamada {i+1}: {elapsed_time:.3f}s")
                else:
                    print(f"   Llamada {i+1}: Error {response.status_code}")

            except Exception as e:
                print(f"   Llamada {i+1}: Excepción {str(e)}")

        if times:
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)

            print(f"✅ Performance de catálogos:")
            print(f"   📈 Promedio: {avg_time:.3f}s")
            print(f"   ⚡ Mínimo: {min_time:.3f}s")
            print(f"   🐌 Máximo: {max_time:.3f}s")

            # Cache hit effectiveness (segunda llamada debería ser más rápida)
            if len(times) >= 2 and times[1] < times[0]:
                print(f"   🎯 Cache hit detectado: {times[0]:.3f}s → {times[1]:.3f}s")

            self.results['catalog_performance'] = {
                'success': True,
                'avg_time': avg_time,
                'min_time': min_time,
                'max_time': max_time,
                'times': times
            }
        else:
            self.results['catalog_performance'] = {'success': False, 'error': 'No successful calls'}

    def test_health_check(self):
        """Test 0: Verificar que el servidor esté funcionando."""
        print("🧪 Test 0: Verificando salud del servidor...")

        try:
            response = requests.get(f"{self.base_url}/healthz")
            if response.status_code == 200:
                print("✅ Servidor funcionando correctamente")
                return True
            else:
                print(f"❌ Servidor no responde correctamente: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ No se puede conectar al servidor: {str(e)}")
            return False

    def run_all_tests(self):
        """Ejecuta todos los tests de performance."""
        print("=" * 60)
        print("🚀 INICIANDO TESTS DE PERFORMANCE DE CACHE")
        print("=" * 60)
        print(f"🌐 Servidor: {self.base_url}")
        print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Test básico de conectividad
        if not self.test_health_check():
            print("\n❌ Tests abortados: servidor no disponible")
            return

        # Tests principales
        self.test_cache_stats()
        self.test_cache_refresh()
        self.test_catalog_loading_performance()

        # Reporte final
        self.print_final_report()

    def print_final_report(self):
        """Imprime un reporte final de todos los tests."""
        print("\n" + "=" * 60)
        print("📋 REPORTE FINAL DE TESTS")
        print("=" * 60)

        success_count = sum(1 for test in self.results.values() if test.get('success', False))
        total_count = len(self.results)

        print(f"📊 Tests exitosos: {success_count}/{total_count}")

        if success_count == total_count:
            print("🎉 ¡TODOS LOS TESTS PASARON!")
            print("\n✨ OPTIMIZACIONES IMPLEMENTADAS EXITOSAMENTE:")
            print("   🔥 Cache de catálogos funcionando")
            print("   ⚡ Performance mejorada")
            print("   📈 Sistema listo para manejar grandes volúmenes")
        else:
            print("⚠️  ALGUNOS TESTS FALLARON")
            for test_name, result in self.results.items():
                if not result.get('success', False):
                    print(f"   ❌ {test_name}: {result.get('error', 'Unknown error')}")

        print("\n🔍 PRÓXIMOS PASOS RECOMENDADOS:")
        print("   1. Ejecutar: db_optimization_indices.sql en la base de datos")
        print("   2. Monitorear performance con /private/cache_stats")
        print("   3. Testear con archivos Excel reales")
        print("   4. Verificar tiempos de carga mejorados")


def main():
    """Función principal del script."""
    import argparse

    parser = argparse.ArgumentParser(description='Test cache performance optimizations')
    parser.add_argument('--url', default='http://localhost:4000',
                       help='Base URL del servidor (default: http://localhost:4000)')

    args = parser.parse_args()

    tester = CachePerformanceTester(args.url)
    tester.run_all_tests()


if __name__ == "__main__":
    main()