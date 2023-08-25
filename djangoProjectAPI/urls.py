"""
URL configuration for djangoProjectAPI project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path

from flight import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/login', views.login),
    # path('api/signup', views.signup),
    path('api/signup', views.manageUser),
    path('api/logout', views.logout),
    path('api/bdd', views.getBDDNames),
    path('api/<str:bdd>/updateCell', views.updateCell),
    path('api/<str:bdd>/manageTable', views.manageTable),
    path('api/<str:bdd>/manageColumn', views.manageColumn),
    path('api/<str:bdd>/manageRow', views.manageRow),
    path('api/<str:bdd>/allforeignkeys', views.getForeignKeysForAllTables),
    path('api/<str:bdd>/allprimarykeys', views.getPrimaryKeysForAllTables),
    path('api/<str:bdd>/tables', views.getNameTables),
    path('api/<str:bdd>/<str:table_name>/colonnes', views.getNameColumns),
    path('api/<str:bdd>/<str:table_name>/notNullColonnes', views.getNotNullColumns),
    path('api/<str:bdd>/<str:table_name>/count', views.getCount),
    path('api/<str:bdd>/<str:table_name>/primarykey', views.getPrimaryKey),
    path('api/<str:bdd>/<str:table_name>/foreignkeys', views.getForeignKeys),
    path('api/<str:bdd>/<str:table_name>/colonne/<str:column_name>/<str:column_value>', views.getDataByColumnValue),
    path('api/<str:bdd>/<str:table_name>/colonne/<str:column_name>', views.getAllInfoColumn),
    path('api/<str:bdd>/<str:table_name>/ligne/<str:row_id>', views.getRow),
    path('api/<str:bdd>/<str:table_name>/all', views.getAllInfoTable),
    path('api/<str:bdd>/<str:table_name>', views.getTableData),
    # path('api/<str:bdd>/<str:table_name>', views.getInfoTable),
]



