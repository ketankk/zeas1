-- phpMyAdmin SQL Dump
-- version 3.5.1
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: Dec 11, 2014 at 07:23 AM
-- Server version: 5.5.24-log
-- PHP Version: 5.4.3

create database IF NOT EXISTS taphius;
use taphius;

SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `taphius`
--

-- --------------------------------------------------------

--
-- Table structure for table `data_ingestion`
--

DROP TABLE IF EXISTS `data_ingestion`;
CREATE TABLE IF NOT EXISTS `data_ingestion` (
  `DATA_INGESTION_ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `NAME` varchar(16) NOT NULL,
  `ARCHIVED_TIME` timestamp NULL DEFAULT NULL,
  `BATCH_START` varchar(16) NOT NULL,
  `BATCH_END` varchar(16) DEFAULT NULL,
  `CURRENT_BATCH` varchar(16) NOT NULL,
  `LAST_BATCH` varchar(16) DEFAULT NULL,
  `CREATED` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `CREATED_BY` varchar(16) DEFAULT NULL,
  `LAST_MODIFIED` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `UPDATED_BY` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`DATA_INGESTION_ID`)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 COMMENT='This will have entries only for “active” Ingestion Entities in the ENTITY table' AUTO_INCREMENT=3 ;

--
-- Dumping data for table `data_ingestion`
--

INSERT INTO `data_ingestion` (`DATA_INGESTION_ID`, `NAME`, `ARCHIVED_TIME`, `BATCH_START`, `BATCH_END`, `CURRENT_BATCH`, `LAST_BATCH`, `CREATED`, `CREATED_BY`, `LAST_MODIFIED`, `UPDATED_BY`) VALUES
(1, 'D_Injestion', '2014-11-14 14:39:41', '2014-11-14 20:09', '2014-11-14 20:09', '2014-11-14 20:09', '2014-11-14 20:09', '2014-11-12 18:30:00', 'user', '2014-11-12 18:30:00', 'user'),
(2, 'More_Injestion', '2014-11-14 14:39:41', '2014-11-14 20:09', '2014-11-14 20:09', '2014-11-14 20:09', '2014-11-14 20:09', '2014-11-13 18:30:00', 'user', '2014-11-13 18:30:00', 'user');

-- --------------------------------------------------------

--
-- Table structure for table `data_ingestion_log`
--

CREATE TABLE IF NOT EXISTS `data_ingestion_log` (
  `DATA_INGESTION_LOG_ID` bigint(20) NOT NULL AUTO_INCREMENT,
  `DATA_INGESTION_ID` bigint(20) NOT NULL COMMENT 'Foreign Key to DATA_INGESTION table',
  `BATCH` varchar(16) NOT NULL,
  `listOfFiles` varchar(2000) NOT NULL,
  `JOB_START_TIME` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `JOB_END_TIME` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `JOB_STATUS` varchar(16) NOT NULL,
  `JOB_MSG` varchar(256) DEFAULT NULL,
  `CREATED` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `CREATED_BY` varchar(16) DEFAULT NULL,
  `LAST_MODIFIED` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `UPDATED_BY` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`DATA_INGESTION_LOG_ID`)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 COMMENT='This  table will have one row per batch run (either success or failure)' AUTO_INCREMENT=3 ;

--
-- Dumping data for table `data_ingestion_log`
--

INSERT INTO `data_ingestion_log` (`DATA_INGESTION_LOG_ID`, `DATA_INGESTION_ID`, `BATCH`, `listOfFiles`, `JOB_START_TIME`, `JOB_END_TIME`, `JOB_STATUS`, `JOB_MSG`, `CREATED`, `CREATED_BY`, `LAST_MODIFIED`, `UPDATED_BY`) VALUES
(1, 6991, '', '', '2014-12-05 12:12:32', '0000-00-00 00:00:00', '', '', '0000-00-00 00:00:00', NULL, '0000-00-00 00:00:00', NULL),
(2, 6991, '', 'a,b', '2014-12-29 09:57:02', '0000-00-00 00:00:00', 'READY', '', '2014-12-29 09:54:21', NULL, '0000-00-00 00:00:00', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `entity`
--

DROP TABLE IF EXISTS `entity`;
CREATE TABLE IF NOT EXISTS `entity` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `NAME` varchar(16) NOT NULL,
  `TYPE` varchar(16) NOT NULL,
  `JSON_DATA` text NOT NULL,
  `IS_ACTIVE` tinyint(1) NOT NULL,
  `CREATED` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `CREATED_BY` varchar(16) DEFAULT NULL,
  `LAST_MODIFIED` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `UPDATED_BY` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 COMMENT='This table will be interface between front-endand the back-end' AUTO_INCREMENT=7018 ;

--
-- Dumping data for table `entity`
--

INSERT INTO `entity` (`ID`, `NAME`, `TYPE`, `JSON_DATA`, `IS_ACTIVE`, `CREATED`, `CREATED_BY`, `LAST_MODIFIED`, `UPDATED_BY`) VALUES
(6988, 'InsuranceSchema', 'DataSchema', '{"sourcerType":"int","id":6988,"name":"InsuranceSchema","description":"Details of Insurance feed","dataSourcerId":"InsuranceSchema","dataAttribute":[{"Name":"policyNum","dataType":"int","description":"Policy Number","maxlen":"5654654","minlen":"565","piirule":"Obfuscate","valRule":"No","active":true,"PII":true},{"dataType":"int","Name":"Policy Name","maxlen":"15","minlen":"10","description":"Policy Name","piirule":"Obfuscate","valRule":"YES","active":true},{"dataType":"int","piirule":"Obfuscate","valRule":"YES","active":false}]}', 1, '2014-11-21 10:37:11', 'manohar', '2014-12-04 12:18:44', 'sharmistha'),
(6989, 'InsuranceDS', 'DataSource', '{"schema":"InsuranceSchema","sourcerType":"File","format":"CSV","id":6989,"name":"InsuranceDS","location":"/home/hadoop/taphius/data/insurance","description":"Details about input data","dataSourcerId":"InsuranceDS"}', 1, '2014-11-21 10:38:10', 'manohar', '2014-11-26 05:21:53', 'sharmistha'),
(6990, 'InsuranceDataSet', 'DataSet', '{"name":"InsuranceDataSet","description":"Details about where the data to be strored on HDFS","Schema":"InsuranceSchema","location":"/lab/taphius/insurance","batchStructure":"YYYY-MM-DD","dataSourcerId":"InsuranceDataSet","id":6990}', 1, '2014-11-21 10:39:30', 'manohar', '2014-11-24 05:40:05', 'manohar'),
(6991, 'InsuranceFeed', 'DataIngestion', '{"frequency":"Hourly","dataSource":"InsuranceDS","name":"InsuranceFeed","startBatch":"2014-11-25","endBatch":"2014-12-31","destinationDataset":"InsuranceDataSet","dataIngestionId":"InsuranceFeed"}', 1, '2014-11-21 10:40:32', 'manohar', '2014-11-21 11:40:32', 'manohar'),
(6997, 'AirportDataSchem', 'DataSchema', '{"sourcerType":"int","id":6997,"name":"AirportDataSchema","description":"AirportDataSchema","dataSourcerId":"AirportDataSchema","dataAttribute":[{"Name":"AirportID","dataType":"int","description":"AirportID","active":true,"piirule":"Obfuscate","valRule":"YES"},{"Name":"Name","dataType":"varchar","description":"Name","piirule":"Obfuscate","valRule":"YES","active":true},{"Name":"City","dataType":"varchar","description":"City","piirule":"Obfuscate","valRule":"YES"}]}', 1, '2014-11-26 16:39:48', 'manohar', '2014-12-04 07:35:03', 'sharmistha'),
(6998, 'AirportDataSrc', 'DataSource', '{"sourcerType":"File","schema":"AirportDataSchem","format":"CSV","id":"","name":"AirportDataSrc","location":"/home/hadoop/taphius/data/airport","description":"AirportDataSrc","dataSourcerId":"AirportDataSrc"}', 1, '2014-11-26 16:41:43', 'manohar', '2014-11-26 16:41:43', 'manohar'),
(7016, 'testdata', 'DataSchema', '{"sourcerType":"int","id":7016,"name":"testdata","description":"testdata description","dataSourcerId":"testdata","dataAttribute":[{"dataType":"varchar","valRule":"YES","Name":"name","description":"name desc","PII":true,"piirule":"Obfuscate","minlen":1,"maxlen":10,"active":true,"valStrict":true},{"dataType":"int","valRule":"YES","Name":"emp id","description":"emp id desc","PII":true,"piirule":"Remove","minVal":2,"maxVal":10,"active":true,"valStrict":false}]}', 1, '2014-12-02 10:22:32', 'sharmistha', '2014-12-11 06:52:43', 'sharmistha'),
(7017, 'REST', 'DataIngestion', '{"frequency":"Hourly","dataSource":"InsuranceDS","name":"REST","startBatch":"2014-10-10","endBatch":"2014-10-12","destinationDataset":"InsuranceDataSet","dataIngestionId":"REST"}', 1, '2014-12-03 12:26:00', 'user', '2014-12-03 12:26:00', 'user');

-- --------------------------------------------------------



--
-- Table structure for table `user`
--

DROP TABLE IF EXISTS `User`;
CREATE TABLE IF NOT EXISTS `User` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(16) NOT NULL,
  `password` varchar(80) NOT NULL,
  `email` varchar(100) NOT NULL,
  `display_name` varchar(200) NOT NULL,
  `CREATED` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `CREATED_BY` varchar(16) DEFAULT NULL,
  `LAST_MODIFIED` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `UPDATED_BY` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `UK_syftr7gx86fwf7ox7bgvnnta7` (`name`)
) ENGINE=InnoDB  DEFAULT CHARSET=latin1 AUTO_INCREMENT=1 ;

--
-- Dumping data for table `user`
--

INSERT INTO `User` (`id`, `name`, `password`, `email`, `display_name`, `CREATED`, `CREATED_BY`, `LAST_MODIFIED`, `UPDATED_BY`) VALUES
(1, 'user', 'fd65ab63ff9bd7bd1ba2c6968f2234d33ce554080d39a8f4034f7adba3cbb38d0ed431d6ab72c1aa', 'user12@u.com', 'Ben Johnson', '2014-11-19 11:40:09', NULL, '0000-00-00 00:00:00', NULL),
(2, 'nivi', 'dcb93889a734e13c4a917e00db8923471595a6db909d35eb96268e4c5f560cfcedca3b445c4e6b6c', 'string@nici.nici', 'nivi242343', '2014-11-25 13:18:18', NULL, '0000-00-00 00:00:00', NULL),
(4, 'Test', 'dcb93889a734e13c4a917e00db8923471595a6db909d35eb96268e4c5f560cfcedca3b445c4e6b6c', 'Test@testt.comaaa', 'TestMinedd', '0000-00-00 00:00:00', NULL, '0000-00-00 00:00:00', NULL),
(101, 'vineet', '2ca2be19c0f9541127a8d5027704db272f6f7374c338d960489eefd91771ace0b3b4c9c1481676e1', 'vineet@taphius.com', 'Vineet Kumar', '2014-11-21 09:29:42', NULL, '0000-00-00 00:00:00', NULL),
(102, 'sharmistha', '38d45891d4586fd58c02f70af9267c72a7bd67768ae85c68a6b447f478d3087b1497c64cedbe3983', 'sharmistha@taphius.com', 'Sharmistha', '2014-11-21 09:54:05', NULL, '0000-00-00 00:00:00', NULL),
(103, 'nivedita', 'f340eb79255dfea985f914c509d81cde72d02422467b56f38e43e84bcdfe4e166b8404733d5b46bb', 'nivedita@taphius.com', 'Nivedita', '2014-11-21 09:55:29', NULL, '0000-00-00 00:00:00', NULL),
(104, 'pravin', '009c42e120b49140f046a033b9034ecc4ad7c606cfd8ffa44086b8fd36eac5f7b194c3471f4bb6f8', 'pravin@taphius.com', 'Pravin Kumar', '2014-11-21 09:56:59', NULL, '0000-00-00 00:00:00', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `user_roles`
--

DROP TABLE IF EXISTS `User_roles`;
CREATE TABLE IF NOT EXISTS `User_roles` (
  `User_id` bigint(20) NOT NULL,
  `roles` varchar(255) DEFAULT NULL,
  `CREATED` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `CREATED_BY` varchar(16) DEFAULT NULL,
  `LAST_MODIFIED` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `UPDATED_BY` varchar(16) DEFAULT NULL,
  KEY `FK_9npctppqlup1uag8ek04qpmie` (`User_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `user_roles`
--

INSERT INTO `User_roles` (`User_id`, `roles`, `CREATED`, `CREATED_BY`, `LAST_MODIFIED`, `UPDATED_BY`) VALUES
(1, 'user', '0000-00-00 00:00:00', NULL, '0000-00-00 00:00:00', NULL),
(4, 'admin', '0000-00-00 00:00:00', NULL, '0000-00-00 00:00:00', NULL),
(2, 'admin', '2014-11-26 04:37:35', NULL, '0000-00-00 00:00:00', NULL),
(101, 'user', '2014-11-21 09:51:56', NULL, '0000-00-00 00:00:00', NULL),
(102, 'user', '2014-11-21 09:58:19', NULL, '0000-00-00 00:00:00', NULL),
(103, 'user', '2014-11-21 09:58:25', NULL, '0000-00-00 00:00:00', NULL),
(104, 'user', '2014-11-21 10:00:42', NULL, '0000-00-00 00:00:00', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `whitelist_config`
--

DROP TABLE IF EXISTS `whitelist_config`;
CREATE TABLE IF NOT EXISTS `whitelist_config` (
  `ID` bigint(20) NOT NULL,
  `NAME` varchar(32) NOT NULL,
  `DISPLAY_NAME` varchar(32) NOT NULL,
  `CONTAINER` varchar(16) DEFAULT NULL,
  `WIDGET_TYPE` varchar(16) NOT NULL,
  `ENTRY` varchar(16) NOT NULL,
  `CREATED` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `CREATED_BY` varchar(16) NOT NULL,
  `LAST_MODIFIED` timestamp NULL DEFAULT NULL,
  `MODIFIED_BY` varchar(16) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT='This table is used to contain values used to populated list in UI.';

--
-- Dumping data for table `whitelist_config`
--

INSERT INTO `whitelist_config` (`ID`, `NAME`, `DISPLAY_NAME`, `CONTAINER`, `WIDGET_TYPE`, `ENTRY`, `CREATED`, `CREATED_BY`, `LAST_MODIFIED`, `MODIFIED_BY`) VALUES
(1, 'Type', 'Type', 'DataSource', 'list', 'File', '2014-11-18 06:09:28', 'user', '2014-11-18 06:09:28', NULL),
(2, 'Type', 'Type', 'DataSource', 'list', 'RDBMS', '2014-11-18 06:09:28', 'user', '2014-11-18 06:09:28', NULL),
(3, 'Format', 'Format', 'DataSource', 'list', 'CSV', '2014-11-18 06:11:54', 'user', '2014-11-18 06:11:54', NULL),
(4, 'Format', 'Format', 'DataSource', 'list', 'XML', '2014-11-18 06:11:54', 'user', '2014-11-18 06:11:54', NULL),
(5, 'Type', 'Type', 'DataSchema', 'list', 'int', '2014-11-18 09:13:12', '', '2014-11-18 09:12:54', NULL),
(6, 'Type', 'Type', 'DataSchema', 'list', 'varchar', '2014-11-18 09:13:22', '', '2014-11-18 09:12:54', NULL),
(7, 'Type', 'Type', 'DataSchema', 'list', 'enumeration', '2014-12-03 08:59:48', 'user', '2014-12-03 08:59:48', NULL);

--
-- Constraints for dumped tables
--

--
-- Constraints for table `user_roles`
--
ALTER TABLE `User_roles`
  ADD CONSTRAINT `FK_9npctppqlup1uag8ek04qpmie` FOREIGN KEY (`User_id`) REFERENCES `user` (`id`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

/*Table structure for schema_archive*/

DROP TABLE IF EXISTS schema_archive;
CREATE TABLE IF NOT EXISTS schema_archive (
  SCHEMA_ID int(11) NOT NULL,
  SCHEMA_NAME varchar(50) DEFAULT NULL,
  SCHEMA_JSON text NOT NULL,
  SOURCE_JSON text NOT NULL,
  DATASET_JSON text NOT NULL,
  SCHEDULAR_JSON text NOT NULL,
  USER_NAME varchar(50) NOT NULL,
  CREATED_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (SCHEMA_ID)
);
