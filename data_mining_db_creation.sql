
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema DataMining
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema DataMining
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `DataMining` DEFAULT CHARACTER SET utf8 ;
USE `DataMining` ;

-- -----------------------------------------------------
-- Table `DataMining`.`topics`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `DataMining`.`topics` (
  `id` INT UNSIGNED NOT NULL COMMENT 'Technical id used as primary key.',
  `subject` VARCHAR(45) NOT NULL COMMENT 'Topic subject',
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `subject_UNIQUE` (`subject` ASC) VISIBLE,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `DataMining`.`publications_types`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `DataMining`.`publications_types` (
  `type_code` INT UNSIGNED NOT NULL COMMENT 'Code of publication tyoe',
  `type_name` VARCHAR(45) NOT NULL COMMENT 'Name of publication type.',
  PRIMARY KEY (`type_code`),
  UNIQUE INDEX `code_UNIQUE` (`type_code` ASC) VISIBLE,
  UNIQUE INDEX `type_name_UNIQUE` (`type_name` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `DataMining`.`publications`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `DataMining`.`publications` (
  `id` INT UNSIGNED NOT NULL COMMENT 'Unique publication identifier extracted from its URL.',
  `pub_type_code` INT UNSIGNED NOT NULL,
  `title` VARCHAR(150) NOT NULL COMMENT 'Title of publication',
  `year` INT UNSIGNED NOT NULL COMMENT 'Year of publication.',
  `citations` INT UNSIGNED NOT NULL COMMENT 'Number of citations of the publication',
  `reads` INT UNSIGNED NOT NULL COMMENT 'Number of publication reads',
  `url` VARCHAR(150) NOT NULL COMMENT 'URL of the publication',
  `journal` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `url_UNIQUE` (`url` ASC) VISIBLE,
  UNIQUE INDEX `title_UNIQUE` (`title` ASC) VISIBLE,
  INDEX `pub_type_code_fk_idx` (`pub_type_code` ASC) VISIBLE,
  CONSTRAINT `pub_type_code_fk`
    FOREIGN KEY (`pub_type_code`)
    REFERENCES `DataMining`.`publications_types` (`type_code`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `DataMining`.`authors`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `DataMining`.`authors` (
  `id` INT UNSIGNED NOT NULL COMMENT 'Technical id',
  `full_name` VARCHAR(45) NOT NULL COMMENT 'full name of author',
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `DataMining`.`publications_by_topics`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `DataMining`.`publications_by_topics` (
  `id` INT UNSIGNED NOT NULL COMMENT 'Technical id',
  `topic_id` INT UNSIGNED NOT NULL COMMENT 'Topic id - FK',
  `pub_id` INT UNSIGNED NOT NULL COMMENT 'Foreign Key = publications.id, taken from publications table. ',
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `pub_id_UNIQUE` (`pub_id` ASC) VISIBLE,
  UNIQUE INDEX `topic_id_UNIQUE` (`topic_id` ASC) VISIBLE,
  CONSTRAINT `topic_id_fk1`
    FOREIGN KEY (`topic_id`)
    REFERENCES `DataMining`.`topics` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `pub_id_fk`
    FOREIGN KEY (`pub_id`)
    REFERENCES `DataMining`.`publications` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `DataMining`.`publications_by_authors`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `DataMining`.`publications_by_authors` (
  `id` INT UNSIGNED NOT NULL COMMENT 'Index',
  `pub_id` INT UNSIGNED NOT NULL COMMENT 'FK =  publications.id from publications table.',
  `author_id` INT UNSIGNED NOT NULL COMMENT 'FK  = authors.id taken from authors table',
  PRIMARY KEY (`id`),
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
  UNIQUE INDEX `pub_id_UNIQUE` (`pub_id` ASC) VISIBLE,
  UNIQUE INDEX `author_id_UNIQUE` (`author_id` ASC) VISIBLE,
  CONSTRAINT `pub_id_fk2`
    FOREIGN KEY (`pub_id`)
    REFERENCES `DataMining`.`publications` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `author_id_fk`
    FOREIGN KEY (`author_id`)
    REFERENCES `DataMining`.`authors` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
