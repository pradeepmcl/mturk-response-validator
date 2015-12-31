insert into users_bonus(id, mturk_id, created_phase, completion_code)
  select id, mturk_id, created_phase, completion_code from users 
    where completion_code is not null and created_phase = 1;
