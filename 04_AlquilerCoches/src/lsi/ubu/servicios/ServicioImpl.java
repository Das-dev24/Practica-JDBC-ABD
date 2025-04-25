package lsi.ubu.servicios;

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
	        con.setAutoCommit(false);
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
	        st_checkCliente = con.prepareStatement("SELECT COUNT(*) FROMclientes WHERE NIF = ?");
	        st_checkCliente.setString(1,nifCliente);
	        rs_checkCliente = st_checkCliente.executeQuery();
	        
	        if(rs_checkCliente.getInt(1) == 0) {
	        	throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);//Lanzamos la excepcion;
	        }
	        
	        st_checkVehiculo = con.prepareStatement("SELECT COUNT(*) FROM vehiculos WHERE matricula = ?");
	        st_checkVehiculo.setString(1,matricula);
	        rs_checkVehiculo = st_checkVehiculo.executeQuery();
	        
	        if(rs_checkVehiculo.getInt(1) == 0) {
	        	throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);//Lanzamos la excepcion;
	        }
	        
	        
			
		} catch (SQLException e) {
			// Completar por el alumno

			LOGGER.debug(e.getMessage());

			throw e;

		} finally {
			/* A rellenar por el alumnado*/
		}
	}
}
