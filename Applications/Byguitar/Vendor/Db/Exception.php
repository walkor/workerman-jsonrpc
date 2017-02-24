<?php
namespace Db;

/**
 * Exceptions about database.Exception is bundled with {@link \Lib\Db} in the
 * same script file.<br />
 * Exception code as below:<br/>
 * <b>Error code start with "42" are database exceptions.</b>
 * <pre>
 * <code>
 * 42000 => connection failure
 * 42001 => no connections available
 * 42003 => specified connecton configuration not found
 * 42004 => Query failure.
 * 42005 => logical eror. bad sql etc.
 * 42101 => Global transaction conflicts.
 * </code>
 * </pre>
 */
class Exception extends \Exception {
}
