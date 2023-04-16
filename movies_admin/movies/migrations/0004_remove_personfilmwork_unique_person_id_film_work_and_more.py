# Generated by Django 4.2 on 2023-04-16 14:15

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0003_alter_filmwork_rating'),
    ]

    operations = [
        migrations.RemoveConstraint(
            model_name='personfilmwork',
            name='unique_person_id_film_work',
        ),
        migrations.AddIndex(
            model_name='personfilmwork',
            index=models.Index(models.F('person'), name='person_idx'),
        ),
        migrations.AddConstraint(
            model_name='personfilmwork',
            constraint=models.UniqueConstraint(fields=('film_work', 'person', 'role'), name='unique_person_id_film_work'),
        ),
    ]
