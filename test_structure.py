#!/usr/bin/env python3
"""Test script to validate pipeline structure without PySpark dependencies"""

import sys
import os

def test_imports():
    """Test if all modules can be imported"""
    try:
        # Test basic imports
        import pipeline
        print("✅ pipeline package imported successfully")
        
        import pipeline.bronze
        print("✅ bronze package imported successfully")
        
        import pipeline.silver
        print("✅ silver package imported successfully")
        
        import pipeline.gold
        print("✅ gold package imported successfully")
        
        import pipeline.dags
        print("✅ dags package imported successfully")
        
        import pipeline.config
        print("✅ config package imported successfully")
        
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False

def test_file_structure():
    """Test if all required files exist"""
    required_files = [
        'pipeline/__init__.py',
        'pipeline/main.py',
        'pipeline/bronze/__init__.py',
        'pipeline/bronze/bronze_layer.py',
        'pipeline/silver/__init__.py',
        'pipeline/silver/silver_layer.py',
        'pipeline/gold/__init__.py',
        'pipeline/gold/gold_layer.py',
        'pipeline/dags/__init__.py',
        'pipeline/dags/healthcare_pipeline.py',
        'pipeline/config/__init__.py',
        'pipeline/config/optimization.py',
        'pipeline/tests/__init__.py',
        'pipeline/tests/test_bronze_layer.py',
        'pipeline/tests/test_silver_layer.py',
        'pipeline/tests/test_gold_layer.py',
        'pipeline/tests/test_integration.py',
        'data/patients.csv',
        'requirements.txt',
        'README.md',
        '.github/workflows/ci.yml'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        print(f"❌ Missing files: {missing_files}")
        return False
    else:
        print("✅ All required files present")
        return True

def main():
    """Main test function"""
    print("🔍 Testing Pipeline Structure...")
    
    imports_ok = test_imports()
    structure_ok = test_file_structure()
    
    if imports_ok and structure_ok:
        print("\n✅ All structure tests passed!")
        return 0
    else:
        print("\n❌ Some structure tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())
