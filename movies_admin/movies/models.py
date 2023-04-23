import uuid
from django.db import models
from django.db.models import UniqueConstraint
from django.db.models import Index
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class FilmTypes(models.TextChoices):
    MOVIE = 'MOV', _('Movie')
    TV_SHOW = 'TVS', _('TV show')


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False
    )

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class FilmWork(UUIDMixin, TimeStampedMixin):
    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)
    creation_date = models.DateField(_('date of creation'), null=True, blank=True)
    rating = models.FloatField(
        _('rating'),
        blank=True,
        null=True,
        validators=[
            MinValueValidator(0),
            MaxValueValidator(100)
        ]
    )
    type = models.TextField(
        _('type'),
        choices=FilmTypes.choices,
        default=FilmTypes.MOVIE
    )
    genres = models.ManyToManyField(Genre, through='GenreFilmWork')
    persons = models.ManyToManyField('Person', through='PersonFilmWork')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('Film production')
        verbose_name_plural = _('Film productions')

    def __str__(self):
        return self.title


class GenreFilmWork(UUIDMixin):
    film_work = models.ForeignKey(
        FilmWork,
        on_delete=models.CASCADE
    )
    genre = models.ForeignKey(
        Genre,
        on_delete=models.CASCADE
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        constraints = [
            UniqueConstraint(
                fields=['film_work', 'genre'],
                name='unique_film_work_genre'
            ),
        ]


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(
        _('full name'),
        max_length=255
    )

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('Participant of the film')
        verbose_name_plural = _('Participants of the film')

    def __str__(self):
        return self.full_name


class PersonFilmWork(UUIDMixin):
    class RoleTypes(models.TextChoices):
        ACTOR = 'ACT', _('Actor')
        PRODUCER = 'PRD', _('Producer')
        DIRECTOR = 'DRC', _('Director')

    person = models.ForeignKey(
        Person,
        on_delete=models.CASCADE,
    )
    film_work = models.ForeignKey(
        FilmWork,
        on_delete=models.CASCADE
    )
    role = models.TextField(
        _('role'),
        choices=RoleTypes.choices,
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        constraints = [
            UniqueConstraint(
                fields=['film_work', 'person', 'role'],
                name='unique_film_work_person_id_role'
            ),
        ]