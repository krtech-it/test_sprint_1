# Generated by Django 4.2 on 2023-04-18 18:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0004_alter_filmwork_creation_date'),
    ]

    operations = [
        migrations.AlterField(
            model_name='personfilmwork',
            name='role',
            field=models.TextField(choices=[('ACT', 'Actor'), ('PRD', 'Producer'), ('DRC', 'Director')], verbose_name='role'),
        ),
    ]
