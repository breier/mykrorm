<?php

/**
 * PHP Version 7
 *
 * Micro ORM File
 *
 * @category Extended_Class
 * @package  Breier\Libs
 * @author   Andre Breier <breier.de@gmail.com>
 * @license  GPLv3 https://www.gnu.org/licenses/gpl-3.0.en.html
 */

namespace Breier;

use PDO;
use PDOException;
use PDOStatement;
use Breier\ExtendedArray;
use Breier\MykrORM\Exception\DBException;

/**
 * Micro ORM Model class
 */
abstract class MykrORM
{
    /**
     * @var PDO Connection
     */
    private $dbConn;

    /**
     * @var array PDO Options
     */
    private $dbOptions = [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_NAMED,
    ];

    /** @var string DB Table Name */
    protected $dbTableName;

    /** @var ExtendedArray DB Properties (table columns) */
    protected $dbProperties;

    /** @var array DB Constructor Args */
    protected $dbConstructorArgs = [];

    /**
     * Set defaults
     */
    public function __construct()
    {
        if (empty($this->dbTableName)) {
            $this->dbTableName = basename(
                str_replace('\\', '/', self::camelToSnake(static::class))
            );
        }
    }

    /**
     * Get DSN for PDO Connection [has to be implemented]
     */
    abstract protected function getDSN(): string;

    /**
     * Get DB Properties (ensure it is an ExtendedArray instance)
     */
    protected function getDBProperties(): ExtendedArray
    {
        if ($this->dbProperties instanceof ExtendedArray) {
            return $this->dbProperties;
        }

        $this->dbProperties = new ExtendedArray($this->dbProperties);
        return $this->dbProperties;
    }

    /**
     * Get PDO Connection
     *
     * @throws DBException
     */
    protected function getConnection(): PDO
    {
        if ($this->dbConn instanceof PDO) {
            return $this->dbConn;
        }

        try {
            $dbDSN = $this->getDSN();
            $dsnParams = substr($dbDSN, strpos($dbDSN, ':') + 1);
            parse_str(str_replace(';', '&', $dsnParams), $params);

            $this->dbConn = new PDO(
                $dbDSN,
                $params['username'] ?? $params['user'] ?? null,
                $params['password'] ?? $params['pass'] ?? null,
                $this->dbOptions
            );
        } catch (PDOException $e) {
            throw new DBException($e->getMessage(), $e->getCode(), $e);
        }

        return $this->dbConn;
    }

    /**
     * Automatic Getters for DB fields
     *
     * @return mixed
     * @throws DBException
     */
    public function __get(string $propertyName)
    {
        if (!property_exists($this, $propertyName)) {
            throw new DBException('Property does not exist!');
        }

        $dbFields = $this->getDBProperties()->keys();
        if (!$dbFields->contains(self::camelToSnake($propertyName))) {
            throw new DBException('Property is not DB property!');
        }

        return $this->{$propertyName};
    }

    /**
     * Setter Mapper for DB fields
     *
     * @throws DBException
     */
    public function __set(string $name, $value): void
    {
        if (!property_exists($this, lcfirst(self::snakeToCamel($name)))) {
            throw new DBException('Property does not exist!');
        }

        $dbFields = $this->getDBProperties()->keys();
        if ($dbFields->count() && !$dbFields->contains(self::camelToSnake($name))) {
            throw new DBException('Property is not DB property!');
        }

        $setter = 'set' . self::snakeToCamel($name);
        $this->{$setter}($value);
    }

    /**
     * Snake To Camel case
     */
    final protected static function snakeToCamel(string $string): string
    {
        return str_replace('_', '', ucwords($string, '_'));
    }

    /**
     * Camel To Snake case
     */
    final protected static function camelToSnake(string $string): string
    {
        return strtolower(
            preg_replace(
                '/([a-z])([A-Z0-9])/',
                '$1_$2',
                preg_replace(
                    '/([A-Z0-9]+)([A-Z][a-z])/',
                    '$1_$2',
                    $string
                )
            )
        );
    }

    /**
     * CRUD [Create, Read, Update, Delete]
     */

    /**
     * [Create] Insert new row to this table
     *
     * @throws DBException
     */
    public function create(): void
    {
        $this->createTableIfNotExists();

        $parameters = $this->getProperties()->filter(
            function ($item) {
                return !is_null($item);
            }
        );

        $placeholders = ExtendedArray::fill(0, $parameters->count(), '?');

        $query = "INSERT INTO {$this->dbTableName}"
            . " ({$parameters->keys()->implode(', ')}) VALUES"
            . " ({$placeholders->implode(', ')})";

        $this->getConnection()->beginTransaction();
        try {
            $preparedStatement = $this->getConnection()->prepare($query);
            $this->bindIndexedParams($preparedStatement, $parameters);
            $preparedStatement->execute();
            $this->getConnection()->commit();
        } catch (PDOException $e) {
            $this->getConnection()->rollBack();
            throw new DBException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * [Read] Get Existing Entries
     *
     * @param array|ExtendedArray $criteria
     *
     * @throws DBException
     */
    public function find($criteria): ExtendedArray
    {
        $this->validateCriteria($criteria);
        $criteria = new ExtendedArray($criteria);

        $whereClause = '';
        if ($criteria->count()) {
            $placeholders = $criteria->keys()->map(
                function ($field) {
                    $property = self::camelToSnake($field);
                    return "{$property} = ?";
                }
            )->implode(' AND ');

            $whereClause = " WHERE {$placeholders}";
        }

        try {
            $preparedStatement = $this->getConnection()->prepare(
                "SELECT * FROM {$this->dbTableName}{$whereClause}"
            );
            $this->bindIndexedParams($preparedStatement, $criteria);
            $preparedStatement->execute();

            $result = new ExtendedArray();
            while (
                $row = $preparedStatement->fetchObject(
                    static::class,
                    $this->dbConstructorArgs
                )
            ) {
                $result->append($row);
            }

            return $result;
        } catch (PDOException $e) {
            throw new DBException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * [Update] Change One Entry
     *
     * @param array|ExtendedArray $criteria
     *
     * @throws DBException
     */
    public function update($criteria): void
    {
        $this->validateCriteria($criteria);

        $criteria = new ExtendedArray($criteria);
        if ($criteria->count() === 0) {
            throw new DBException('Criteria cannot be empty!');
        }

        try {
            $originalList = $this->find($criteria);
        } catch (DBException $e) {
            throw new DBException(static::class . ' Not Found!');
        }

        if ($originalList->count() !== 1) {
            throw new DBException("'{$criteria->jsonEncode()}' Not Found!");
        }
        $original = $originalList->first()->element();

        $parameters = $this->getProperties();
        $placeholders = $parameters->keys()->map(
            function ($field) {
                return "{$field} = ?";
            }
        );

        $primaryField = $this->findPrimaryKey();
        $parameters->append($original->{$primaryField->asProperty});

        $query = "UPDATE {$this->dbTableName}"
            . " SET {$placeholders->implode(', ')}"
            . " WHERE {$primaryField->as_db_field} = ?";

        $this->getConnection()->beginTransaction();
        try {
            $preparedStatement = $this->getConnection()->prepare($query);
            $this->bindIndexedParams($preparedStatement, $parameters);
            $preparedStatement->execute();
            $this->getConnection()->commit();
        } catch (PDOException $e) {
            $this->getConnection()->rollBack();
            throw new DBException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * [Delete] Erase Current Entry
     *
     * @throws DBException
     */
    public function delete(): void
    {
        $primaryField = $this->findPrimaryKey();
        $primaryValue = $this->{$primaryField->asProperty};
        if (empty($primaryValue)) {
            throw new DBException("'{$primaryField->asProperty}' is empty!");
        }

        $query = "DELETE FROM {$this->dbTableName}"
            . " WHERE {$primaryField->as_db_field} = ?";

        $this->getConnection()->beginTransaction();
        try {
            $preparedStatement = $this->getConnection()->prepare($query);
            $preparedStatement->execute([$primaryValue]);
            if ($preparedStatement->rowCount() !== 1) {
                throw new PDOException(
                    "'{$primaryField->as_db_field}' was not found or unique!"
                );
            }
            $this->getConnection()->commit();
        } catch (PDOException $e) {
            $this->getConnection()->rollBack();
            throw new DBException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Get Detached DB Property Fields with current values
     *
     * @throws DBException
     */
    protected function getProperties(): ExtendedArray
    {
        $propertyFields = new ExtendedArray($this->getDBProperties());

        foreach ($propertyFields as $field => &$value) {
            $propertyName = lcfirst(self::snakeToCamel($field));
            $value = $this->{$propertyName};
        }

        return $propertyFields;
    }

    /**
     * Bind Statement Parameters using dynamic PDO types
     */
    protected function bindIndexedParams(
        PDOStatement $statement,
        ExtendedArray $parameters
    ): void {
        $index = 0;

        foreach ($parameters as &$value) {
            if (is_null($value)) {
                $statement->bindParam(++$index, $value, PDO::PARAM_NULL);
            } elseif (is_bool($value)) {
                $statement->bindParam(++$index, $value, PDO::PARAM_BOOL);
            } else {
                $statement->bindParam(++$index, $value, PDO::PARAM_STR);
            }
        }
    }

    /**
     * Validate Criteria
     *
     * @param array|ExtendedArray $criteria
     *
     * @throws DBException
     */
    final protected function validateCriteria($criteria): bool
    {
        if (!ExtendedArray::isArray($criteria)) {
            throw new DBException('Invalid criteria format!');
        }

        $criteria = new ExtendedArray($criteria);

        foreach ($criteria->keys() as $field) {
            $property = self::camelToSnake($field);
            if (!$this->getDBProperties()->offsetExists($property)) {
                throw new DBException("Invalid criteria '{$field}'!");
            }
        }

        return true;
    }

    /**
     * Find Primary Key
     */
    protected function findPrimaryKey(): ExtendedArray
    {
        $primaryField = $this->getDBProperties()->keys()->first()->element();

        foreach ($this->getDBProperties() as $field => $type) {
            if (preg_match('/PRIMARY KEY/', strtoupper($type)) === 1) {
                $primaryField = $field;
                break;
            }
        }

        return new ExtendedArray([
            'as_db_field' => $primaryField,
            'asProperty' => lcfirst(self::snakeToCamel($primaryField)),
        ]);
    }

    /**
     * Table Management [automated stuff]
     */

    /**
     * Create this table if it doesn't exist
     * And 'alter' it if necessary
     *
     * @throws DBException
     */
    protected function createTableIfNotExists(): void
    {
        if ($this->getDBProperties()->count() === 0) {
            throw new DBException('Empty DB properties!');
        }

        try {
            $result = $this->getConnection()->query(
                "SELECT * FROM {$this->dbTableName} LIMIT 1"
            );
        } catch (PDOException $e) {
            $this->createTable();
            return;
        }

        $fields = $result->fetch();
        if ($fields === false) {
            return;
        }

        $this->alterTable(
            $this->getDBProperties()->keys()->diff(array_keys($fields))
        );
    }

    /**
     * Create this table
     *
     * @throws DBException
     */
    private function createTable(): void
    {
        $fields = $this->getDBProperties()->map(
            function ($type, $field) {
                return "{$field} {$type}";
            },
            $this->getDBProperties()->keys()->getArrayCopy()
        )->implode(', ');

        try {
            $this->getConnection()->exec(
                "CREATE TABLE {$this->dbTableName} ({$fields})"
            );
        } catch (PDOException $e) {
            throw new DBException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * Alter this table
     *
     * @throws DBException
     */
    private function alterTable(ExtendedArray $diff): void
    {
        if ($diff->count() === 0) {
            return;
        }

        foreach ($diff as $field) {
            try {
                $this->getConnection()->exec(
                    "ALTER TABLE {$this->dbTableName} ADD {$field} "
                    . $this->getDBProperties()->offsetGet($field)
                );
            } catch (PDOException $e) {
                throw new DBException($e->getMessage(), $e->getCode(), $e);
            }
        }
    }
}
