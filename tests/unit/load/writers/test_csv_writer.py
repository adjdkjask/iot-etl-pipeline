"""
Pruebas unitarias para CSVWriter.
"""

import pytest
import pytest_check as check
import pyarrow as pa
from pathlib import Path


class TestCSVWriter:
    """Pruebas para el writer de CSV."""

    @pytest.fixture
    def sample_table(self) -> pa.Table:
        """Tabla PyArrow de ejemplo para tests."""
        return pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [10.5, 20.0, 30.5],
            }
        )

    @pytest.fixture
    def csv_writer(self, tmp_path: Path):
        """Fixture que crea un CSVWriter con directorio temporal."""
        from load.writers.csv_writer import CSVWriter

        return CSVWriter(base_path=tmp_path)

    def test_write_should_create_csv_file(
        self, csv_writer, sample_table: pa.Table, tmp_path: Path
    ):
        """
        Verifica que write() cree un archivo CSV en el directorio base.
        """
        result_path = csv_writer.write(sample_table, "test_table")

        check.is_true(result_path.exists(), "El archivo CSV debe existir")
        check.equal(result_path.suffix, ".csv", "La extensión debe ser .csv")
        check.equal(result_path.name, "test_table.csv", "El nombre debe coincidir")

    def test_write_should_include_all_rows(
        self, csv_writer, sample_table: pa.Table, tmp_path: Path
    ):
        """
        Verifica que el CSV contenga todas las filas de la tabla.
        """
        result_path = csv_writer.write(sample_table, "test_table")

        lines = result_path.read_text().strip().split("\n")
        # 1 header + 3 data rows
        check.equal(len(lines), 4, "Debe haber 4 líneas (1 header + 3 rows)")

    def test_write_should_include_header_by_default(
        self, csv_writer, sample_table: pa.Table, tmp_path: Path
    ):
        """
        Verifica que el CSV incluya encabezados por defecto.
        """
        result_path = csv_writer.write(sample_table, "test_table")

        content = result_path.read_text()
        first_line = content.split("\n")[0]

        check.is_in("id", first_line, "Header debe contener 'id'")
        check.is_in("name", first_line, "Header debe contener 'name'")
        check.is_in("value", first_line, "Header debe contener 'value'")

    def test_write_should_use_custom_delimiter(
        self, tmp_path: Path, sample_table: pa.Table
    ):
        """
        Verifica que se respete el delimitador personalizado.
        """
        from load.writers.csv_writer import CSVWriter

        writer = CSVWriter(base_path=tmp_path, delimiter=";")
        result_path = writer.write(sample_table, "test_table")

        content = result_path.read_text()
        first_line = content.split("\n")[0]

        check.is_in(";", first_line, "Debe usar ';' como delimitador")
        check.is_not_in(",", first_line, "No debe usar ',' como delimitador")

    def test_write_should_exclude_header_when_configured(
        self, tmp_path: Path, sample_table: pa.Table
    ):
        """
        Verifica que se pueda excluir el header.
        """
        from load.writers.csv_writer import CSVWriter

        writer = CSVWriter(base_path=tmp_path, include_header=False)
        result_path = writer.write(sample_table, "test_table")

        lines = result_path.read_text().strip().split("\n")
        # Solo 3 data rows, sin header
        check.equal(len(lines), 3, "Debe haber 3 líneas (solo data rows)")

    def test_write_all_should_create_multiple_files(self, csv_writer, tmp_path: Path):
        """
        Verifica que write_all() cree múltiples archivos CSV.
        """
        tables = {
            "table_a": pa.table({"col1": [1, 2]}),
            "table_b": pa.table({"col2": ["x", "y"]}),
        }

        results = csv_writer.write_all(tables)

        check.equal(len(results), 2, "Debe retornar 2 paths")
        check.is_true(results["table_a"].exists(), "table_a.csv debe existir")
        check.is_true(results["table_b"].exists(), "table_b.csv debe existir")

    def test_write_all_should_return_correct_paths(self, csv_writer, tmp_path: Path):
        """
        Verifica que write_all() retorne los paths correctos.
        """
        tables = {
            "dim_date": pa.table({"date_sk": [1]}),
            "fact_sales": pa.table({"amount": [100.0]}),
        }

        results = csv_writer.write_all(tables)

        check.equal(results["dim_date"].name, "dim_date.csv")
        check.equal(results["fact_sales"].name, "fact_sales.csv")

    def test_constructor_should_create_base_directory(self, tmp_path: Path):
        """
        Verifica que el constructor cree el directorio base si no existe.
        """
        from load.writers.csv_writer import CSVWriter

        new_dir = tmp_path / "exports" / "csv"
        writer = CSVWriter(base_path=new_dir)

        check.is_true(new_dir.exists(), "El directorio base debe ser creado")

    def test_write_should_preserve_data_types_in_content(
        self, csv_writer, sample_table: pa.Table, tmp_path: Path
    ):
        """
        Verifica que los valores se escriban correctamente.
        """
        result_path = csv_writer.write(sample_table, "test_table")

        content = result_path.read_text()

        check.is_in("Alice", content, "Debe contener valor 'Alice'")
        check.is_in("10.5", content, "Debe contener valor '10.5'")
