"""
Pruebas unitarias para Loader.
"""

import pytest
import pytest_check as check
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Union


class MockWriter:
    """Writer mock para pruebas del Loader."""

    def __init__(self):
        self.written_tables: Dict[str, pa.Table] = {}

    def write(self, table: pa.Table, table_name: str) -> Path:
        self.written_tables[table_name] = table
        return Path(f"/mock/{table_name}.csv")

    def write_all(self, tables: Dict[str, pa.Table]) -> Dict[str, Union[Path, str]]:
        results = {}
        for name, table in tables.items():
            results[name] = self.write(table, name)
        return results


class TestLoader:
    """Pruebas para el orquestador Loader."""

    @pytest.fixture
    def parquet_source(self, tmp_path: Path) -> Path:
        """
        Crea un directorio temporal con tablas Parquet de ejemplo.
        """
        source_dir = tmp_path / "output"
        source_dir.mkdir()

        # Crear dim_date
        dim_date_dir = source_dir / "dim_date"
        dim_date_dir.mkdir()
        dim_date = pa.table({"date_sk": [20240101, 20240102], "year": [2024, 2024]})
        pq.write_table(dim_date, dim_date_dir / "data.parquet")

        # Crear fact_sales
        fact_sales_dir = source_dir / "fact_sales"
        fact_sales_dir.mkdir()
        fact_sales = pa.table({"sale_id": [1, 2], "amount": [100.0, 200.0]})
        pq.write_table(fact_sales, fact_sales_dir / "data.parquet")

        return source_dir

    @pytest.fixture
    def mock_writer(self) -> MockWriter:
        """Writer mock para capturar las tablas escritas."""
        return MockWriter()

    @pytest.fixture
    def loader(self, parquet_source: Path, mock_writer: MockWriter):
        """Loader configurado con source y writer de prueba."""
        from load.loader import Loader

        return Loader(source_path=parquet_source, writer=mock_writer)

    def test_list_tables_should_return_available_tables(
        self, loader, parquet_source: Path
    ):
        """
        Verifica que list_tables() retorne los nombres de las tablas disponibles.
        """
        tables = loader.list_tables()

        check.equal(len(tables), 2, "Debe encontrar 2 tablas")
        check.is_in("dim_date", tables, "Debe incluir dim_date")
        check.is_in("fact_sales", tables, "Debe incluir fact_sales")

    def test_list_tables_should_return_empty_for_nonexistent_path(self, mock_writer):
        """
        Verifica que list_tables() retorne lista vacía si el path no existe.
        """
        from load.loader import Loader

        loader = Loader(source_path=Path("/nonexistent/path"), writer=mock_writer)
        tables = loader.list_tables()

        check.equal(tables, [], "Debe retornar lista vacía")

    def test_list_tables_should_ignore_empty_directories(
        self, parquet_source: Path, mock_writer
    ):
        """
        Verifica que list_tables() ignore directorios sin archivos Parquet.
        """
        from load.loader import Loader

        # Crear directorio vacío
        empty_dir = parquet_source / "empty_table"
        empty_dir.mkdir()

        loader = Loader(source_path=parquet_source, writer=mock_writer)
        tables = loader.list_tables()

        check.is_not_in("empty_table", tables, "No debe incluir directorios vacíos")

    def test_load_should_write_all_tables_when_no_filter(
        self, loader, mock_writer: MockWriter
    ):
        """
        Verifica que load() sin parámetros cargue todas las tablas.
        """
        results = loader.load()

        check.equal(len(results), 2, "Debe cargar 2 tablas")
        check.is_in("dim_date", mock_writer.written_tables)
        check.is_in("fact_sales", mock_writer.written_tables)

    def test_load_should_filter_tables_when_specified(
        self, loader, mock_writer: MockWriter
    ):
        """
        Verifica que load() solo cargue las tablas especificadas.
        """
        results = loader.load(tables=["dim_date"])

        check.equal(len(results), 1, "Debe cargar solo 1 tabla")
        check.is_in("dim_date", mock_writer.written_tables)
        check.is_not_in("fact_sales", mock_writer.written_tables)

    def test_load_should_return_writer_results(self, loader):
        """
        Verifica que load() retorne los paths/identificadores del writer.
        """
        results = loader.load()

        check.equal(results["dim_date"], Path("/mock/dim_date.csv"))
        check.equal(results["fact_sales"], Path("/mock/fact_sales.csv"))

    def test_load_single_should_write_one_table(self, loader, mock_writer: MockWriter):
        """
        Verifica que load_single() cargue una única tabla.
        """
        result = loader.load_single("dim_date")

        check.equal(len(mock_writer.written_tables), 1, "Debe escribir solo 1 tabla")
        check.is_in("dim_date", mock_writer.written_tables)
        check.equal(result, Path("/mock/dim_date.csv"))

    def test_load_single_should_raise_for_nonexistent_table(self, loader):
        """
        Verifica que load_single() lance error si la tabla no existe.
        """
        with pytest.raises(FileNotFoundError) as exc_info:
            loader.load_single("nonexistent_table")

        check.is_in("nonexistent_table", str(exc_info.value))

    def test_read_table_should_return_pyarrow_table(self, loader, parquet_source: Path):
        """
        Verifica que _read_table() retorne un PyArrow Table válido.
        """
        table = loader._read_table("dim_date")

        check.is_instance(table, pa.Table, "Debe retornar PyArrow Table")
        check.equal(table.num_rows, 2, "Debe tener 2 filas")
        check.is_in("date_sk", table.column_names, "Debe tener columna date_sk")

    def test_read_table_should_raise_for_nonexistent_table(self, loader):
        """
        Verifica que _read_table() lance error si la tabla no existe.
        """
        with pytest.raises(FileNotFoundError) as exc_info:
            loader._read_table("nonexistent")

        check.is_in("nonexistent", str(exc_info.value))

    def test_has_parquet_files_should_detect_parquet_in_subdirs(
        self, parquet_source: Path, mock_writer
    ):
        """
        Verifica que _has_parquet_files() detecte Parquet en subdirectorios.
        """
        from load.loader import Loader

        # Crear estructura con Parquet en subdirectorio (particionado)
        partitioned_dir = parquet_source / "partitioned_table"
        partitioned_dir.mkdir()
        partition_subdir = partitioned_dir / "year=2024"
        partition_subdir.mkdir()
        pq.write_table(pa.table({"value": [1]}), partition_subdir / "part-0.parquet")

        loader = Loader(source_path=parquet_source, writer=mock_writer)

        check.is_true(
            loader._has_parquet_files(partitioned_dir),
            "Debe detectar Parquet en subdirectorios",
        )

    def test_loader_should_accept_path_as_string(
        self, parquet_source: Path, mock_writer
    ):
        """
        Verifica que el Loader acepte el path como string.
        """
        from load.loader import Loader

        loader = Loader(source_path=str(parquet_source), writer=mock_writer)
        tables = loader.list_tables()

        check.greater(len(tables), 0, "Debe funcionar con path como string")
