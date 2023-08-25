<?php

namespace PdoDbal;

interface DBInterface
{
    public function query($sql, ...$parameters);

    public function prepare($query, ...$parameters);

    public function results($primaryKey = null);

    public function result($column = null);

    public function lastInsertId();

}