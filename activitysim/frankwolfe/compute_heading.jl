using Geodesy

function compute_heading(from::LLA, to::LLA)::Float32
    # # https://towardsdatascience.com/calculating-the-bearing-between-two-geospatial-coordinates-66203f57e4b4
    # ΔL = to.lon - from.lon
    # x = cosd(to.lat) * sind(ΔL)
    # y = cosd(from.lat) * sind(to.lat) - sind(from.lat) * cosd(to.lat) * cosd(ΔL)
    # bearing = atand(x, y)

    # multiplying longitude by cos(lat) scales so angles are comparable
    # you could do something a lot more complicated but there's no reason to
    lon_scale = cosd((from.lat + to.lat) / 2)
    Δlat = to.lat - from.lat
    Δlon = (to.lon - from.lon) * lon_scale

    bearing = atand(Δlon, Δlat) % 360

    if bearing < 0
        bearing += 360
    end

    return convert(Float32, bearing)::Float32
end

function circular_add(bearing::Real, Δ::Real)::Float32
    new_bearing = bearing + Δ
    return convert(Float32, new_bearing % 360)::Float32
end

function heading_between(ang1, ang2)
    # return the angle betwen ang1 and ang2. will be positive if ang2 is to the right of ang1
    @assert ang1 >= 0 && ang1 < 360 "invalid ang1 $ang1"
    @assert ang2 >= 0 && ang2 < 360 "invalid ang2 $ang2"
    Δhdg = ang2 - ang1
    if Δhdg > 180
        Δhdg -= 360
    elseif Δhdg < -180
        Δhdg += 360
    end

    @assert abs(Δhdg) <= 180 "$ang1 -> $ang2 yields invalid delta heading $Δhdg"

    return Δhdg
end