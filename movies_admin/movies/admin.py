from django.contrib import admin
from .models import Genre
from .models import FilmWork
from .models import Person
from .models import GenreFilmWork
from .models import PersonFilmWork


class GenreFilmInline(admin.TabularInline):
    model = GenreFilmWork


class PersonFilmInline(admin.TabularInline):
    model = PersonFilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name', 'uid')


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmInline, PersonFilmInline)

    list_display = ('title', 'type', 'creation_date', 'rating')
    list_filter = ('type', 'creation_date')
    search_fields = ('title', 'description', 'uid')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'gender')
    search_fields = ('full_name', 'uid')
    list_filter = ('gender',)
