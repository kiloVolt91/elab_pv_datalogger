-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema mydb
-- -----------------------------------------------------
-- -----------------------------------------------------
-- Schema db_fotovoltaico_asi
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `db_fotovoltaico_asi` ;

-- -----------------------------------------------------
-- Schema db_fotovoltaico_asi
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `db_fotovoltaico_asi` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci ;
USE `db_fotovoltaico_asi` ;

-- -----------------------------------------------------
-- Table `db_fotovoltaico_asi`.`inverter`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_fotovoltaico_asi`.`inverter` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `modello` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `idInverter_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `db_fotovoltaico_asi`.`datalogger_inverter`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_fotovoltaico_asi`.`datalogger_inverter` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `local_time` TIMESTAMP NOT NULL,
  `fk_address` INT NOT NULL,
  `Inv_State` FLOAT NULL DEFAULT NULL,
  `T_WR` FLOAT NULL DEFAULT NULL,
  `Seq_State` FLOAT NULL DEFAULT NULL,
  `P_AC` FLOAT NULL DEFAULT NULL,
  `E_AC` FLOAT NULL DEFAULT NULL,
  `E_DAY` FLOAT NULL DEFAULT NULL,
  `I_AC` FLOAT NULL DEFAULT NULL,
  `U_AC` FLOAT NULL DEFAULT NULL,
  `F_AC` FLOAT NULL DEFAULT NULL,
  `U_LV` FLOAT NULL DEFAULT NULL,
  `COS_PHI` FLOAT NULL DEFAULT NULL,
  `U_grid` FLOAT NULL DEFAULT NULL,
  `I_grid` FLOAT NULL DEFAULT NULL,
  `I_dc` FLOAT NULL DEFAULT NULL,
  `E_int` FLOAT NULL DEFAULT NULL,
  `time_utc` TIMESTAMP NOT NULL,
  `timestamp_utc` BIGINT NOT NULL,
  `Irr` FLOAT NULL DEFAULT NULL,
  `temp_media_string_box` FLOAT NULL DEFAULT NULL,
  INDEX `ix_inverter_index` (`id` ASC) VISIBLE,
  INDEX `fk_address_idx` (`fk_address` ASC) VISIBLE,
  PRIMARY KEY (`id`, `timestamp_utc`, `time_utc`, `local_time`, `fk_address`),
  UNIQUE INDEX `index_UNIQUE` (`id` ASC) VISIBLE,
  CONSTRAINT `fk_inv_address`
    FOREIGN KEY (`fk_address`)
    REFERENCES `db_fotovoltaico_asi`.`inverter` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_as_cs;


-- -----------------------------------------------------
-- Table `db_fotovoltaico_asi`.`campo_fotovoltaico`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_fotovoltaico_asi`.`campo_fotovoltaico` (
  `id` INT NOT NULL,
  `indirizzo stringa` INT NOT NULL,
  `fk_inverter` INT NOT NULL,
  PRIMARY KEY (`id`, `fk_inverter`),
  UNIQUE INDEX `indirizzo stringa_UNIQUE` (`indirizzo stringa` ASC) VISIBLE,
  INDEX `fk_inverter_idx` (`fk_inverter` ASC) VISIBLE,
  CONSTRAINT `fk_inverter`
    FOREIGN KEY (`fk_inverter`)
    REFERENCES `db_fotovoltaico_asi`.`inverter` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `db_fotovoltaico_asi`.`datalogger_stringa`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `db_fotovoltaico_asi`.`datalogger_stringa` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `local_time` TIMESTAMP NOT NULL,
  `fk_address` INT NOT NULL,
  `I_AC_av` FLOAT NULL DEFAULT NULL,
  `I_DC0` FLOAT NULL DEFAULT NULL,
  `I_DC1` FLOAT NULL DEFAULT NULL,
  `I_DC2` FLOAT NULL DEFAULT NULL,
  `I_DC3` FLOAT NULL DEFAULT NULL,
  `I_DC4` FLOAT NULL DEFAULT NULL,
  `I_DC5` FLOAT NULL DEFAULT NULL,
  `I_DC6` FLOAT NULL DEFAULT NULL,
  `I_DC7` FLOAT NULL DEFAULT NULL,
  `T_AMB` FLOAT NULL DEFAULT NULL,
  `T_PAN` FLOAT NULL DEFAULT NULL,
  `T_CARD` FLOAT NULL DEFAULT NULL,
  `time_utc` TIMESTAMP NOT NULL,
  `timestamp_utc` BIGINT NOT NULL,
  INDEX `ix_stringhe_index` (`id` ASC) VISIBLE,
  PRIMARY KEY (`id`, `local_time`, `time_utc`, `timestamp_utc`, `fk_address`),
  UNIQUE INDEX `index_UNIQUE` (`id` ASC) VISIBLE,
  INDEX `fk_address_idx` (`fk_address` ASC) VISIBLE,
  CONSTRAINT `fk_string_address`
    FOREIGN KEY (`fk_address`)
    REFERENCES `db_fotovoltaico_asi`.`campo_fotovoltaico` (`indirizzo stringa`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

-- -----------------------------------------------------
-- Data for table `db_fotovoltaico_asi`.`inverter`
-- -----------------------------------------------------
START TRANSACTION;
USE `db_fotovoltaico_asi`;
INSERT INTO `db_fotovoltaico_asi`.`inverter` (`id`, `modello`) VALUES (1, 'Gefran APV270TL');

COMMIT;


-- -----------------------------------------------------
-- Data for table `db_fotovoltaico_asi`.`campo_fotovoltaico`
-- -----------------------------------------------------
START TRANSACTION;
USE `db_fotovoltaico_asi`;
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (1, 101, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (2, 102, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (3, 103, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (4, 104, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (5, 105, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (6, 106, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (7, 107, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (8, 108, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (9, 109, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (10, 110, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (11, 111, 1);
INSERT INTO `db_fotovoltaico_asi`.`campo_fotovoltaico` (`id`, `indirizzo stringa`, `fk_inverter`) VALUES (12, 112, 1);

COMMIT;

