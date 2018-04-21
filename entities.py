from django.db import models

class program_type(models.Model):
    id = models.BigAutoField(primary_key=True)
    code = models.CharField(default = 'OT',max_length = 8)
    name = models.CharField(default='',max_length=32)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True,auto_now=True)


class game_genre(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=32,)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True,auto_now=True)


class program(models.Model):
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(default='',max_length=256)
    kana_title = models.CharField(default='',max_length=256)
    other_title_01 = models.CharField(default='',max_length=256)
    other_title_02 = models.CharField(default='',max_length=256)
    anisoninfo_program_id = models.BigIntegerField()
    program_type_id = models.ForeignKey(program_type,on_delete=models.PROTECT)
    game_genre_id = models.ForeignKey(game_genre,on_delete=models.PROTECT)
    broadcast_start_on = models.DateField(default='0000-00-00') 
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True,auto_now=True)

class song_role(models.Model):
    id = models.BigAutoField(primary_key=True)
    code = models.CharField(default='OT',max_length=8)
    name = models.CharField(max_length=32)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True,auto_now=True)


class singer(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=256)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True,auto_now=True)


class song(models.Model):
    id = models.BigAutoField(primary_key=True)
    title = models.CharField(default='',max_length=256)
    released_on = models.DateField(default='0000-00-00')
    program_id = models.ForeignKey(program,on_delete=models.PROTECT)
    anisoninfo_song_id = models.BigIntegerField()
    song_role_id = models.ForeignKey(song_role,on_delete=models.PROTECT)
    singer_id = models.ForeignKey(singer,on_delete=models.PROTECT)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True,auto_now=True)

