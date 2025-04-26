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
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	private static final int DIAS_DE_ALQUILER = 4;

	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		Connection con = null;
		PreparedStatement st_checkCliente = null;
		ResultSet rs_checkCliente = null;
		
		PreparedStatement st_checkVehiculo = null;
		ResultSet rs_checkVehiculo = null;
		
		PreparedStatement st_checkDisponible = null;
		ResultSet rs_checkDisponible = null;
		
		PreparedStatement st_Insert = null;
		
		PreparedStatement st_getDatosModelo = null;
		ResultSet rs_getDatosModelo = null;
		
		/*
		 * El calculo de los dias se da hecho
		 */
		long diasDiff = DIAS_DE_ALQUILER;
		if (fechaFin != null) {
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());

			if (diasDiff < 1) {
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
		}

		try {
			con = pool.getConnection();
			LOGGER.debug("Conexión obtenida del pool");
	        con.setAutoCommit(false);
	        LOGGER.debug("Se desactiva el autocommit");
	        /* A completar por el alumnado... */

			/* ================================= AYUDA R�PIDA ===========================*/
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
	        
	        //Verificamos que existe el cliente
	        int client_exists_index = 1;
	        st_checkCliente = con.prepareStatement("SELECT COUNT(*) FROM clientes WHERE NIF = ?");
	        st_checkCliente.setString(client_exists_index, nifCliente);
	        rs_checkCliente = st_checkCliente.executeQuery();
	        
	        //Verificamos que el coche existe
	        int car_exists_index = 1;
	        st_checkVehiculo = con.prepareStatement("SELECT COUNT(*) FROM vehiculos WHERE matricula = ?");
	        st_checkVehiculo.setString(car_exists_index++,matricula);
	        rs_checkVehiculo = st_checkVehiculo.executeQuery();
	        
	        if (rs_checkVehiculo.next() && rs_checkVehiculo.getInt(1)==0) {
	        	throw new AlquilerCochesException (AlquilerCochesException.VEHICULO_NO_EXIST);
	        }
	        
	        if (rs_checkCliente.next()) {
	        	int nClientes = rs_checkCliente.getInt(1);
	        	if(nClientes == 0) {
	        		throw new AlquilerCochesException ( AlquilerCochesException.CLIENTE_NO_EXIST);
	        	} 
	        }
	        


	        
	        //Verificamos que el coche está disponible
	        int free_car_index = 1;
	        java.sql.Date sqlIni = new java.sql.Date(fechaIni.getTime());
	        java.sql.Date sqlFin = (fechaFin == null) ? 
	        		new java.sql.Date(sqlIni.getTime() + DIAS_DE_ALQUILER*86_400_000L) :
	        		new java.sql.Date(fechaFin.getTime());
	        st_checkDisponible = con.prepareStatement(
	        	    "SELECT COUNT(*) FROM reservas WHERE matricula = ? AND (" + 
	        		"(fecha_fin >= ? AND fecha_ini <= ?) OR " +
	        	    "(fecha_fin >= ? AND fecha_ini <= ?  ) OR " +
	        		"(fecha_fin >= ? AND fecha_ini <= ?  ) OR" +
	        	    "(fecha_fin >= ? AND fecha_ini >= ? ))");
	        st_checkDisponible.setString(free_car_index++,matricula);
	        st_checkDisponible.setDate(free_car_index++,sqlIni);
	        st_checkDisponible.setDate(free_car_index++,sqlFin);
	        st_checkDisponible.setDate(free_car_index++,sqlIni);
	        st_checkDisponible.setDate(free_car_index++,sqlIni);
	        st_checkDisponible.setDate(free_car_index++,sqlFin);
	        st_checkDisponible.setDate(free_car_index++,sqlIni);
	        st_checkDisponible.setDate(free_car_index++,sqlFin);
	        st_checkDisponible.setDate(free_car_index++,sqlIni);
	        rs_checkDisponible = st_checkDisponible.executeQuery();
	        
	        if (rs_checkDisponible.next() && rs_checkDisponible.getInt(1)>0) {
	        	throw new AlquilerCochesException (AlquilerCochesException.VEHICULO_OCUPADO);
	        }

	        int insert_index = 1;
	        st_Insert = con.prepareStatement("INSERT INTO reservas VALUES (seq_reservas.nextval,?,?,?,?)");
	        st_Insert.setString(insert_index++,nifCliente);
	        st_Insert.setString(insert_index++,matricula);
	        st_Insert.setDate(insert_index++, sqlIni);
	        st_Insert.setDate(insert_index++, sqlFin);
	        int filasMod= st_Insert.executeUpdate();
	        
	        if (filasMod == 0) {
	        	throw new SQLException();
	        }
	        
	        
	        st_getDatosModelo = con.prepareStatement("SELECT m.id_modelo, m.nombre, m.precio_cada_dia, m.capacidad_deposito, m.tipo_combustible, pc.precio_por_litro " +
	            "FROM vehiculos v " +
	            "JOIN modelos m ON v.id_modelo = m.id_modelo " +
	            "JOIN precio_combustible pc ON m.tipo_combustible = pc.tipo_combustible " +
	            "WHERE v.matricula = ?");
	        
	        st_getDatosModelo.setString(1, matricula);
	        rs_getDatosModelo = st_getDatosModelo.executeQuery();
	        
	        if (!rs_getDatosModelo.next()) {
	            throw new SQLException("No se pudo obtener información del modelo del vehículo");
	        }
	        
	        // Obtener los datos del modelo
	        int idModelo = rs_getDatosModelo.getInt("id_modelo");
	        String nombreModelo = rs_getDatosModelo.getString("nombre");
	        BigDecimal precioPorDia = rs_getDatosModelo.getBigDecimal("precio_cada_dia");
	        int capacidadDeposito = rs_getDatosModelo.getInt("capacidad_deposito");
	        String tipoCombustible = rs_getDatosModelo.getString("tipo_combustible");
	        BigDecimal precioPorLitro = rs_getDatosModelo.getBigDecimal("precio_por_litro");
	        
	        
	        con.commit();
	        
			
		} catch (AlquilerCochesException e) {
			if(con!=null) con.rollback();
			throw e;
			
		} catch (SQLException e) {
			try {
				if(con!=null) con.rollback();
			} catch (SQLException rollbackEx) {
				LOGGER.error("Error al hacer rollback", rollbackEx);
			}


			
			//LOGGER.error("Error SQL 2: " + e.getMessage());

			throw e;

		} finally {
			/* A rellenar por el alumnado*/
			  // Cerrar todos los recursos
	        try {
	            if (rs_checkCliente != null) rs_checkCliente.close();
	            if (rs_checkVehiculo != null) rs_checkVehiculo.close();
	            if (rs_checkDisponible != null) rs_checkDisponible.close();
	            if (st_checkCliente != null) st_checkCliente.close();
	            if (st_checkVehiculo != null) st_checkVehiculo.close();
	            if (st_checkDisponible != null) st_checkDisponible.close();
	            if (st_Insert != null) st_Insert.close();
	            if (con != null) con.close();
	        } catch (SQLException e) {
	            LOGGER.error("Error al cerrar recursos: " + e.getMessage());
	        }
		}
	}
}
