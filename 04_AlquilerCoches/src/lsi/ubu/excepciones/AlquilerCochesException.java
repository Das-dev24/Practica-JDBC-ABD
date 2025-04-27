package lsi.ubu.excepciones;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AlquierCochesException: Implementa las excepciones contextualizadas de la
 * transaccion de alquiler de coches
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesus Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raul Marticorena</a>
 * @author <a href="mailto:srarribas@ubu.es">Sandra Rodríguez</a>
 * @version 1.2
 * @since 1.0
 */
public class AlquilerCochesException extends SQLException {

    // Identificador único para la serialización
    private static final long serialVersionUID = 1L;

    // Logger para registrar eventos y errores
    private static final Logger LOGGER = LoggerFactory.getLogger(AlquilerCochesException.class);

    // Códigos de error para las diferentes excepciones
    public static final int CLIENTE_NO_EXIST = 1; // Cliente no existe
    public static final int VEHICULO_NO_EXIST = 2; // Vehículo no existe
    public static final int SIN_DIAS = 3; // Número de días inválido
    public static final int VEHICULO_OCUPADO = 4; // Vehículo no disponible

    private int codigo; // Código de error
    private String mensaje; // Mensaje de error

    /**
     * Constructor de la clase AlquilerCochesException.
     *
     * @param code Código de error de la excepción
     */
    public AlquilerCochesException(int code) {

        /*
         * A completar por el alumnado
         */

        // Determinar el mensaje de error según el código
        switch (code) {
            case CLIENTE_NO_EXIST: // Si el cliente no existe
                this.mensaje = "ERROR: " + code + " Cliente inexistente";
                this.codigo = code;
                break;

            case VEHICULO_NO_EXIST: // Si el vehículo no existe
                this.mensaje = "ERROR: " + code + " Vehículo inexistente";
                this.codigo = code;
                break;

            case SIN_DIAS: // Si el número de días es inválido
                this.mensaje = "ERROR: " + code + "El número de días será mayor que cero";
                this.codigo = code;
                break;

            case VEHICULO_OCUPADO: // Si el vehículo no está disponible
                this.mensaje = "ERROR: " + code + "El vehículo no está disponible";
                this.codigo = code;
        }

        // Registrar el mensaje de error en el logger
        LOGGER.debug(mensaje);

        // Traza_de_pila
        /*
         * for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
         * LOGGER.debug(ste.toString()); }
         */
    }

    /**
     * Método para obtener el mensaje de error.
     *
     * @return El mensaje de error
     */
    @Override
    public String getMessage() { // Redefinicion del metodo de la clase Exception
        return mensaje;
    }

    /**
     * Método para obtener el código de error.
     *
     * @return El código de error
     */
    @Override
    public int getErrorCode() { // Redefinicion del metodo de la clase SQLException
        return codigo;
    }
}
