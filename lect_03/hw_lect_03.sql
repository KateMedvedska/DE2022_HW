/*
 Завдання на SQL до лекції 02.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
select
    category.name AS category,
    count(film_category.film_id)  AS films_amount
from category
         left join film_category on category.category_id = film_category.category_id
group by category.name
order by films_amount desc;



/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
select
    a.first_name,
    a.last_name,
    sum(rental_id) as qty
from rental r
        left join inventory i on r.inventory_id = i.inventory_id
        left join film_actor fa on i.film_id = fa.film_id
        left join actor a on fa.actor_id = a.actor_id
where a.actor_id is not null
group by a.first_name, a.last_name
order by qty desc
limit 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
--Якщо потрібна завжди тільки одна катеогорія, тоді можна використати наступну вибірку:
select
    c.name as category,
    sum(p.amount) as payments
from payment p
        left join rental r on p.rental_id = r.rental_id
        left join inventory i on r.inventory_id = i.inventory_id
        left join film_category fc on i.film_id = fc.film_id
        left join category c on fc.category_id = c.category_id
group by c.name
order by payments desc
limit 1;

/*
Якщо розуміємо, що категорій з максимальною оплатою можу бути декілька,
    тоді краще використати наступний запит
*/
with payments_by_category as (
    /*
    Виберемо суми оплат по всім категоріям фільмів
    */
    select
        c.name as category,
        sum(p.amount) as payments
    from payment p
            left join rental r on p.rental_id = r.rental_id
            left join inventory i on r.inventory_id = i.inventory_id
            left join film_category fc on i.film_id = fc.film_id
            left join category c on fc.category_id = c.category_id
    group by c.name
    order by payments desc
)

select
    category,
    payments
from payments_by_category
where payments = (select max(payments) from payments_by_category);



/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
select
    film.title
from film
    left join inventory i on film.film_id = i.film_id
where i.inventory_id is null;



/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
/*
Аналогічно завданню 3:
   Якщо необхідно вивести рівно 3 актори, тоді використовуємо наступний запит:

Примітка: обовʼязково групуємо по actor_id, оскільки можуть бути різні актори
з однаковими іменами
*/
select
    a.first_name,
    a.last_name,
    count(1) as qty
from category c
        left join film_category fc on c.category_id = fc.category_id
        left join film_actor fa on fc.film_id = fa.film_id
        left join actor a on fa.actor_id = a.actor_id
where c.name = 'Children'
group by a.actor_id, a.first_name, a.last_name
order by qty desc, a.first_name, a.last_name
limit 3;

/*
Якщо необхідно вивести всіх акторів, які посідають перші три місьця по кількості появ у фільмах,
тоді використовуємо наступний запит:
*/
with actors_in_child_films as (
    /*
    Порахуємо скільки раз всі актори зʼявлялись у фільмах категорії Children
    */
    select
        a.actor_id,
        a.first_name,
        a.last_name,
        count(1) as qty
    from category c
            left join film_category fc on c.category_id = fc.category_id
            left join film_actor fa on fc.film_id = fa.film_id
            left join actor a on fa.actor_id = a.actor_id
    where c.name = 'Children'
    group by a.actor_id, a.first_name, a.last_name
    order by qty desc, a.first_name, a.last_name
)

select
    actor_id,
    first_name,
    last_name,
    qty
from actors_in_child_films
where qty in (
        select distinct
            qty
        from actors_in_child_films
        order by qty desc
        limit 3
    );
/*
Якщо ж прибрати distinct у відборі в кінці попереднього запиту,
то можна отримати топ 3 акторів + всіх акторів з такою ж кількістю
появ у дитячих фільмах, як і у перших трьох акторів.
*/


/*
6.
Вивести міста з кількістю активних та неактивних клієнтів
(в активних customer.active = 1).
Результат відсортувати за кількістю неактивних клієнтів за спаданням.
*/
select
    city.city,
    sum(c.active) as active_customers,
    sum(case when c.active = 0 then 1 else 0 end) as inactive_customers
from customer c
        left join address a on c.address_id = a.address_id
        left join city on city.city_id = a.city_id
group by city.city_id, city.city
order by inactive_customers desc;

