package lsi.ubu.servicios;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;

public class ServicioImpl implements Servicio {
	// Logger para registrar eventos y errores
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	// Días de alquiler por defecto si no se especifica la fecha de fin
	private static final int DIAS_DE_ALQUILER = 4;

	/**
	 * Método para alquilar un coche a un cliente.
	 *
	 * @param nifCliente NIF del cliente
	 * @param matricula Matrícula del coche
	 * @param fechaIni Fecha de inicio del alquiler
	 * @param fechaFin Fecha de fin del alquiler (puede ser nula)
	 * @throws SQLException Si ocurre un error al acceder a la base de datos
	 */
	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		// Obtener una conexión del pool
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		Connection con = null; // Conexión a la base de datos
		PreparedStatement st_checkCliente = null; // Para verificar si el cliente existe
		ResultSet rs_checkCliente = null; // Resultado de la consulta del cliente

		PreparedStatement st_checkVehiculo = null; // Para verificar si el vehículo existe
		ResultSet rs_checkVehiculo = null; // Resultado de la consulta del vehículo

		PreparedStatement st_checkDisponible = null; // Para verificar si el vehículo está disponible
		ResultSet rs_checkDisponible = null; // Resultado de la consulta de disponibilidad

		PreparedStatement st_Insert = null; // Para insertar la reserva

		PreparedStatement st_getDatosModelo = null; // Para obtener datos del modelo del vehículo
		ResultSet rs_getDatosModelo = null; // Resultado de la consulta del modelo

		PreparedStatement st_createFactura = null; // Para crear la factura
		ResultSet rs_createFactura = null; // Resultado de la creación de la factura

		PreparedStatement st_createLineaFactura = null; // Para crear las líneas de la factura

		/*
		 * El cálculo de los días se da hecho
		 */
		long diasDiff = DIAS_DE_ALQUILER; // Días de diferencia entre fechaIni y fechaFin
		if (fechaFin != null) { // Si se proporciona fechaFin
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime()); // Calcula la diferencia en días

			if (diasDiff < 1) { // Si la diferencia es menor que 1
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS); // Lanza excepción indicando que el número de días es inválido
			}
		}

		try {
			con = pool.getConnection(); // Obtiene una conexión del pool
			LOGGER.debug("Conexión obtenida del pool");
			con.setAutoCommit(false); // Desactiva el autocommit para controlar la transacción manualmente
			LOGGER.debug("Se desactiva el autocommit");
			/* A completar por el alumnado... */

			/*
			 * ================================= AYUDA RPIDA
			 * ===========================
			 */
			/*
			 * Algunas de las columnas utilizan tipo numeric en SQL, lo que se traduce en
			 * BigDecimal para Java.
			 *
			 * Convertir un entero en BigDecimal: new BigDecimal(diasDiff)
			 *
			 * Sumar 2 BigDecimals: usar metodo "add" de la clase BigDecimal
			 *
			 * Multiplicar 2 BigDecimals: usar metodo "multiply" de la clase BigDecimal
			 *
			 *
			 * Paso de util.Date a sql.Date java.sql.Date sqlFechaIni = new
			 * java.sql.Date(sqlFechaIni.getTime());
			 *
			 *
			 * Recuerda que hay casos donde la fecha fin es nula, por lo que se debe de
			 * calcular sumando los dias de alquiler (ver variable DIAS_DE_ALQUILER) a la
			 * fecha ini.
			 */

			// Verificamos que el coche existe
			int car_exists_index = 1; // Índice para el parámetro en la consulta
			st_checkVehiculo = con.prepareStatement("SELECT COUNT(*) FROM vehiculos WHERE matricula = ?"); // Preparar la consulta
			st_checkVehiculo.setString(car_exists_index++, matricula); // Asignar el valor de la matrícula
			rs_checkVehiculo = st_checkVehiculo.executeQuery(); // Ejecutar la consulta

			if (rs_checkVehiculo.next() && rs_checkVehiculo.getInt(1) == 0) { // Si no existe el vehículo
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST); // Lanza excepción
			}

			// Verificamos que existe el cliente
			int client_exists_index = 1; // Índice para el parámetro en la consulta
			st_checkCliente = con.prepareStatement("SELECT COUNT(*) FROM clientes WHERE NIF = ?"); // Preparar la consulta
			st_checkCliente.setString(client_exists_index, nifCliente); // Asignar el valor del NIF
			rs_checkCliente = st_checkCliente.executeQuery(); // Ejecutar la consulta

			if (rs_checkCliente.next()) { // Si se obtuvo un resultado
				int nClientes = rs_checkCliente.getInt(1); // Obtiene el número de clientes
				if (nClientes == 0) { // Si no existe el cliente
					throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST); // Lanza excepción
				}
			}

			// Verificamos que el coche está disponible
			int free_car_index = 1; // Índice para el parámetro en la consulta

			// Declaración de fechas mejorada, manteniendo la fecha como null en la BD si se da null.
			java.sql.Date sqlIni = new java.sql.Date(fechaIni.getTime()); // Convierte la fecha de inicio a java.sql.Date
			java.sql.Date sqlFin = null; // Inicializa la fecha de fin como null
			if (fechaFin != null) { // Si se proporciona fechaFin
				sqlFin = new java.sql.Date(fechaFin.getTime()); // Convierte la fecha de fin a java.sql.Date
			}

			// Para verificar disponibilidad, necesitamos una fecha fin temporal si es dada como
			// null
			java.sql.Date sqlFinTemp = (fechaFin == null) // Si la fecha de fin es nula
					? new java.sql.Date(sqlIni.getTime() + DIAS_DE_ALQUILER * 86_400_000L) // Calcula la fecha de fin sumando los días de alquiler por defecto
					: sqlFin; // Si no, usa la fecha de fin proporcionada
			st_checkDisponible = con.prepareStatement( // Preparar la consulta para verificar disponibilidad
					"SELECT COUNT(*) FROM reservas WHERE matricula = ? AND ("
							+ "(fecha_fin >= ? AND fecha_ini <= ?) OR "
							+ "(fecha_fin >= ? AND fecha_ini <= ?  ) OR "
							+ "(fecha_fin >= ? AND fecha_ini <= ?  ) OR"
							+ "(fecha_fin >= ? AND fecha_ini >= ? ))");
			st_checkDisponible.setString(free_car_index++, matricula); // Asignar la matrícula
			st_checkDisponible.setDate(free_car_index++, sqlIni); // Asignar la fecha de inicio
			st_checkDisponible.setDate(free_car_index++, sqlFinTemp); // Asignar la fecha de fin temporal
			st_checkDisponible.setDate(free_car_index++, sqlIni); // Asignar la fecha de inicio
			st_checkDisponible.setDate(free_car_index++, sqlIni); // Asignar la fecha de inicio
			st_checkDisponible.setDate(free_car_index++, sqlFinTemp); // Asignar la fecha de fin temporal
			st_checkDisponible.setDate(free_car_index++, sqlIni); // Asignar la fecha de inicio
			st_checkDisponible.setDate(free_car_index++, sqlFinTemp); // Asignar la fecha de fin temporal
			st_checkDisponible.setDate(free_car_index++, sqlIni); // Asignar la fecha de inicio
			rs_checkDisponible = st_checkDisponible.executeQuery(); // Ejecutar la consulta

			if (rs_checkDisponible.next() && rs_checkDisponible.getInt(1) > 0) { // Si el coche no está disponible
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO); // Lanza excepción
			}

			int insert_index = 1; // Índice para los parámetros de inserción
			st_Insert = con.prepareStatement("INSERT INTO reservas VALUES (seq_reservas.nextval,?,?,?,?)"); // Preparar la consulta de inserción
			st_Insert.setString(insert_index++, nifCliente); // Asignar el NIF del cliente
			st_Insert.setString(insert_index++, matricula); // Asignar la matrícula
			st_Insert.setDate(insert_index++, sqlIni); // Asignar la fecha de inicio
			st_Insert.setDate(insert_index++, sqlFin); // Asignar la fecha de fin
			int filasMod = st_Insert.executeUpdate(); // Ejecutar la inserción

			if (filasMod == 0) { // Si no se insertó ninguna fila
				throw new SQLException(); // Lanza excepción
			}

			st_getDatosModelo = con.prepareStatement( // Preparar la consulta para obtener datos del modelo
					"SELECT m.id_modelo, m.nombre, m.precio_cada_dia, m.capacidad_deposito, m.tipo_combustible, pc.precio_por_litro "
							+ "FROM vehiculos v " + "JOIN modelos m ON v.id_modelo = m.id_modelo "
							+ "JOIN precio_combustible pc ON m.tipo_combustible = pc.tipo_combustible "
							+ "WHERE v.matricula = ?");

			st_getDatosModelo.setString(1, matricula); // Asignar la matrícula
			rs_getDatosModelo = st_getDatosModelo.executeQuery(); // Ejecutar la consulta

			if (!rs_getDatosModelo.next()) { // Si no se encontraron datos del modelo
				throw new SQLException("No se pudo obtener información del modelo del vehículo"); // Lanza excepción
			}

			// Obtener los datos del modelo
			int idModelo = rs_getDatosModelo.getInt("id_modelo"); // Obtiene el ID del modelo
			String nombreModelo = rs_getDatosModelo.getString("nombre"); // Obtiene el nombre del modelo
			BigDecimal precioPorDia = rs_getDatosModelo.getBigDecimal("precio_cada_dia"); // Obtiene el precio por día
			int capacidadDeposito = rs_getDatosModelo.getInt("capacidad_deposito"); // Obtiene la capacidad del depósito
			String tipoCombustible = rs_getDatosModelo.getString("tipo_combustible"); // Obtiene el tipo de combustible
			BigDecimal precioPorLitro = rs_getDatosModelo.getBigDecimal("precio_por_litro"); // Obtiene el precio por litro

			// Calcular el importe de la factura
			BigDecimal importeAlquiler = precioPorDia.multiply(new BigDecimal(diasDiff)); // Calcula el importe del alquiler
			BigDecimal importeDeposito = precioPorLitro.multiply(new BigDecimal(capacidadDeposito)); // Calcula el importe del depósito
			BigDecimal importeTotal = importeAlquiler.add(importeDeposito); // Calcula el importe total

			// Crear la factura - usando secuencia y luego obteniendo el valor
			// Primero obtenemos el valor de la secuencia
			st_createFactura = con.prepareStatement("SELECT seq_num_fact.nextval FROM dual"); // Preparar la consulta para obtener el valor de la secuencia
			rs_createFactura = st_createFactura.executeQuery(); // Ejecutar la consulta

			if (!rs_createFactura.next()) { // Si no se pudo obtener el valor de la secuencia
				throw new SQLException("No se pudo obtener el siguiente valor de la secuencia"); // Lanza excepción
			}

			int nroFactura = rs_createFactura.getInt(1); // Obtener el valor de la secuencia
			rs_createFactura.close(); // Cerrar el ResultSet
			st_createFactura.close(); // Cerrar el PreparedStatement

			// Ahora insertamos la factura con el nroFactura que acabamos de obtener
			int createFactura_index = 1; // Índice para los parámetros de inserción de la factura
			st_createFactura = con.prepareStatement("INSERT INTO facturas VALUES (?, ?, ?)"); // Preparar la consulta de inserción
			st_createFactura.setInt(createFactura_index++, nroFactura); // Asignar el número de factura
			st_createFactura.setBigDecimal(createFactura_index++, importeTotal); // Asignar el importe total
			st_createFactura.setString(createFactura_index++, nifCliente); // Asignar el NIF del cliente
			int filasInsertadasFactura = st_createFactura.executeUpdate(); // Ejecutar la inserción

			if (filasInsertadasFactura == 0) { // Si no se insertó ninguna fila
				throw new SQLException("No se pudo crear la factura"); // Lanza excepción
			}

			// Crear la línea de factura para el alquiler
			String conceptoAlquiler = diasDiff + " dias de alquiler, vehiculo modelo " + idModelo + "   "; // Define el concepto del alquiler
			st_createLineaFactura = con.prepareStatement( // Preparar la consulta para insertar la línea de factura
					"INSERT INTO lineas_factura VALUES (?, ?, ?)");
			st_createLineaFactura.setInt(1, nroFactura); // Asignar el número de factura
			st_createLineaFactura.setString(2, conceptoAlquiler); // Asignar el concepto
			st_createLineaFactura.setBigDecimal(3, importeAlquiler); // Asignar el importe
			st_createLineaFactura.executeUpdate(); // Ejecutar la inserción

			// Crear la línea de factura para el depósito de combustible
			String conceptoDeposito = "Deposito lleno de " + capacidadDeposito + " litros de " + tipoCombustible + " "; // Define el concepto del depósito
			st_createLineaFactura = con.prepareStatement( // Preparar la consulta para insertar la línea de factura
					"INSERT INTO lineas_factura VALUES (?, ?, ?)");
			st_createLineaFactura.setInt(1, nroFactura); // Asignar el número de factura
			st_createLineaFactura.setString(2, conceptoDeposito); // Asignar el concepto
			st_createLineaFactura.setBigDecimal(3, importeDeposito); // Asignar el importe
			st_createLineaFactura.executeUpdate(); // Ejecutar la inserción

			// Confirmar la transacción
			con.commit();

		} catch (AlquilerCochesException e) { // Captura excepciones específicas de la aplicación
			if (con != null)
				con.rollback(); // Si hay una conexión, deshace la transacción
			throw e; // Relanza la excepción
		} catch (SQLException e) { // Captura excepciones de SQL
			try {
				if (con != null)
					con.rollback(); // Si hay una conexión, deshace la transacción
			} catch (SQLException rollbackEx) { // Captura excepciones al hacer rollback
				LOGGER.error("Error al hacer rollback", rollbackEx); // Registra el error
			}

			// LOGGER.error("Error SQL 2: " + e.getMessage());

			throw e; // Relanza la excepción

		} finally {
			/* A rellenar por el alumnado */
			// Cerrar todos los recursos
			try {
				if (rs_checkCliente != null)
					rs_checkCliente.close(); // Cierra el ResultSet del cliente
				if (rs_checkVehiculo != null)
					rs_checkVehiculo.close(); // Cierra el ResultSet del vehículo
				if (rs_checkDisponible != null)
					rs_checkDisponible.close(); // Cierra el ResultSet de disponibilidad
				if (st_checkCliente != null)
					st_checkCliente.close(); // Cierra el PreparedStatement del cliente
				if (st_checkVehiculo != null)
					st_checkVehiculo.close(); // Cierra el PreparedStatement del vehículo
				if (st_checkDisponible != null)
					st_checkDisponible.close(); // Cierra el PreparedStatement de disponibilidad
				if (st_Insert != null)
					st_Insert.close(); // Cierra el PreparedStatement de inserción
				if (con != null)
					con.close(); // Cierra la conexión
			} catch (SQLException e) {
				LOGGER.error("Error al cerrar recursos: " + e.getMessage()); // Registra el error al cerrar recursos
			}
		}
	}
}
