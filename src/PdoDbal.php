<?php

namespace PdoDbal;

use Exception;
use PDO;
use PDOException;
use PDOStatement;

class PdoDbal implements DBInterface
{
    private PDO $pdo;
    private PDOStatement $pdoStatement;
    private static array $dsn = array(
        'mysql' => 'mysql:host=%s;port=%d;dbname=%s',
        'pgsql' => 'pgsql:host=%s;port=%d;dbname=%s',
        'dblib' => 'dblib:host=%s:%d;dbname=%s',
        'mssql' => 'sqlsrv:Server=%s,%d;Database=%s',
        'cubrid' => 'cubrid:host=%s;port=%d;dbname=%s'
    );

    public function __construct($hostname, $username, $password, $dbname, $type = 'mysql', $port = 3306, array $options = [])
    {
        $dsn = sprintf(self::$dsn[$type], $hostname, $port, $dbname);
        if ($type == 'mysql') {
            $options[PDO::MYSQL_ATTR_USE_BUFFERED_QUERY] = true;
            $options[PDO::ATTR_EMULATE_PREPARES] = true;
        }
        try {
            $this->pdo = new PDO($dsn, $username, $password, $options);
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        } catch (PDOException $e) {
            print "Error: " . $e->getMessage();
        }
    }

    /**
     * Execute SQL statement
     * @throws Exception
     */
    public function query($sql, ...$parameters): false|PDOStatement
    {
        $query = empty($parameters) ? $sql : $this->prepare($sql, ...$parameters);
        return $this->pdoStatement = $this->pdo->query($query);
    }

    /*
     * Prepare placeholders in query and returns it secure for execution
     * Types of placeholders:
     *      ?s (string) string escaped through PDO->quote
     *      ?i (integer) integer escaped through intval()
     *      ?f (float) float escaped through floatval() and str_replace comma with dot
     *      ?a (simple array) placeholder for IN operator (convert array to list of string like
     *         'value', 'value'. Strings escaped through PDO->quote)
     *      ?A (associative array) placeholder for SET operator (convert associative array it to
     *         string like `field`='value',`field`='value'. Values escaped through PDO->quote)
     *      ?t table name placeholder return `table`
     *      ?p prepared string
     * Examples:
     *      ->prepare('SELECT * FROM users WHERE group = ?s AND points > ?i', 'user', 7000);
     *      SELECT * FROM users WHERE group = 'user' AND points > 7000
     *
     *      ->prepare('SELECT * FROM user WHERE name IN(?a)', array('foo', 'bar', 'hello', 'world'));
     *      SELECT * FROM user WHERE name IN('foo', 'bar', 'hello', 'world')
     *
     *      ->prepare('INSERT INTO users SET ?A', array('name'=>'User Name', 'group'=>'wholesale', 'points'=>7000));
     *      INSERT INTO users SET `name` = 'User Name', `group` = 'wholesale', `points` = '7000'
     * */

    public function prepare($query, ...$parameters): string
    {
        if (empty($parameters)) return $query;

        $parsed_query = '';
        $array = preg_split('~(\?[sifaAtp])~u', $query, 0, PREG_SPLIT_DELIM_CAPTURE);
        $parameters_num = count($parameters);
        $placeholders_num = floor(count($array) / 2);
        if ($placeholders_num != $parameters_num) {
            throw new Exception(
                "Number of args ($parameters_num) doesn't match number of placeholders ($placeholders_num) in [$query]"
            );
        }
        foreach ($array as $i => $part) {
            if (($i % 2) == 0) {
                $parsed_query .= $part;
                continue;
            }

            $value = array_shift($parameters);
            switch ($part) {
                case '?s':
                    $part = $this->pdo->quote($value);
                    break;
                case '?i':
                    $part = is_int($value) ? $value : intval($value);
                    break;
                case '?f':
                    $part = is_float($value) ? $value : floatval(str_replace(',', '.', $value));
                    break;
                case '?a':
                    if (!is_array($value)) {
                        throw new Exception("?a placeholder expects array, " . gettype($value) . " given");
                    }
                    foreach ($value as &$v) {
                        $v = $this->pdo->quote($v);
                    }
                    $part = implode(',', $value);
                    break;
                case '?A':
                    if (is_array($value) && $value !== array_values($value)) {
                        foreach ($value as $key => &$v) {
                            $v = '`' . $key . '`=' . $this->pdo->quote($v);
                        }
                        $part = implode(', ', $value);
                    } else {
                        throw new Exception("?A placeholder expects Associative array, " . gettype($value) . " given");
                    }
                    break;
                case '?t':
                    $part = '`' . $value . '`';
                    break;
                case '?p':
                    $part = $value;
                    break;
            }
            $parsed_query .= $part;
        }
        return $parsed_query;
    }

    /*
     * Fetches all rows from a result set and return as array of objects.
     * If $primaryKey return associative array of objects
     * */
    public function results($primaryKey = null): false|array
    {
        $results = $this->pdoStatement->fetchAll($this->pdo::FETCH_CLASS);
        if (!empty($primaryKey)) {
            $associativeResults = array();
            foreach ($results as $row) {
                $associativeResults[$row->$primaryKey] = $row;
            }
            return $associativeResults;
        } else {
            return $results;
        }
    }

    /*
     * Fetches one row and returns it as an object
     * If $column is given, returns a single column of a result set
     * */
    public function result($column = null): mixed
    {
        if ($column) {
            $data = $this->pdoStatement->fetch();
            return $data[$column] ?? false;
        } else {
            return $this->pdoStatement->fetchObject();
        }
    }

    public function lastInsertId(): false|string
    {
        return $this->pdo->lastInsertId();
    }

}