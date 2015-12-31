CREATE TABLE `users_bonus` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `mturk_id` varchar(20) NOT NULL,
  `created_phase` tinyint NOT NULL,
  `completion_code` VARCHAR(20),
  `bonus_type` tinyint NOT NULL default 0,
  `bonus_amount` tinyint NOT NULL default 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;