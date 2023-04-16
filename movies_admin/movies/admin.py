from datetime import date
from django.contrib import admin
from django.utils.translation import gettext_lazy as _
from .models import Genre
from .models import FilmWork
from .models import Person
from .models import GenreFilmWork
from .models import PersonFilmWork


class GenreFilmInline(admin.TabularInline):
    model = GenreFilmWork


class PersonFilmInline(admin.TabularInline):
    model = PersonFilmWork


class DateCreatedListFilter(admin.SimpleListFilter):
    title = _('date of creation')
    parameter_name = 'date'

    def lookups(self, request, model_admin):
        return (
            ('before 80s', _('before 80s')),
            ('80s', _('80s')),
            ('90s', _('90s')),
            ('00s', _('00s')),
            ('10s', _('10s')),
            ('20s', _('20s')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'before 80s':
            return queryset.filter(
                creation_date__lte=date(1979, 12, 31)
            )
        if self.value() == '80s':
            return queryset.filter(
                creation_date__gte=date(1980, 1, 1),
                creation_date__lte=date(1989, 12, 31)
            )
        if self.value() == '90s':
            return queryset.filter(
                creation_date__gte=date(1990, 1, 1),
                creation_date__lte=date(1999, 12, 31)
            )
        if self.value() == '00s':
            return queryset.filter(
                creation_date__gte=date(2000, 1, 1),
                creation_date__lte=date(2009, 12, 31)
            )
        if self.value() == '10s':
            return queryset.filter(
                creation_date__gte=date(2010, 1, 1),
                creation_date__lte=date(2019, 12, 31)
            )
        if self.value() == '20s':
            return queryset.filter(
                creation_date__gte=date(2020, 1, 1),
                creation_date__lte=date(2029, 12, 31)
            )


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name', 'uid')


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmInline, PersonFilmInline)

    list_display = ('title', 'type', 'creation_date', 'rating')
    list_filter = ('type', DateCreatedListFilter)
    search_fields = ('title', 'description', 'uid')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ('full_name', 'gender')
    search_fields = ('full_name', 'uid')
    list_filter = ('gender',)
